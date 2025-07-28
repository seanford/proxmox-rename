#!/bin/bash
set -e

# --- Config ---
timestamp=$(date +%Y%m%d_%H%M%S)
backup_dir="/root/pve_rollback_backup_$timestamp"
rollback_log="$backup_dir/rollback.log"
mkdir -p "$backup_dir"

# --- Rollback Function ---
rollback() {
    echo "Error detected. Rolling back changes..."
    # Restore /etc/hosts
    if [ -f "$backup_dir/hosts" ]; then
        cp "$backup_dir/hosts" /etc/hosts
        echo "Restored /etc/hosts"
    fi

    # Restore /etc/pve/nodes
    if [ -d "$backup_dir/nodes" ]; then
        rm -rf /etc/pve/nodes
        cp -a "$backup_dir/nodes" /etc/pve/nodes
        echo "Restored /etc/pve/nodes"
    fi

    # Restore VM and LXC configs
    if [ -d "$backup_dir/qemu-server" ]; then
        rm -rf /etc/pve/nodes/$new_hostname/qemu-server
        cp -a "$backup_dir/qemu-server" /etc/pve/nodes/$old_hostname/
        echo "Restored VM configs"
    fi
    if [ -d "$backup_dir/lxc" ]; then
        rm -rf /etc/pve/nodes/$new_hostname/lxc
        cp -a "$backup_dir/lxc" /etc/pve/nodes/$old_hostname/
        echo "Restored LXC configs"
    fi

    # Restore storage.cfg
    if [ -f "$backup_dir/storage.cfg" ]; then
        cp "$backup_dir/storage.cfg" /etc/pve/storage.cfg
        echo "Restored storage.cfg"
    fi

    # Restore corosync.conf
    if [ -f "$backup_dir/corosync.conf" ]; then
        cp "$backup_dir/corosync.conf" /etc/pve/corosync.conf
        echo "Restored corosync.conf"
    fi

    # Restore /var/lib/rrdcached dirs
    for rrd_dir in node storage vm; do
        base_path="/var/lib/rrdcached/db/pve2-$rrd_dir"
        if [ -d "$backup_dir/pve2-$rrd_dir-$old_hostname" ]; then
            rm -rf "$base_path/$new_hostname"
            mv "$backup_dir/pve2-$rrd_dir-$old_hostname" "$base_path/$old_hostname"
            echo "Restored $base_path/$old_hostname"
        fi
    done

    echo "Rollback complete. Please check your system."
    exit 1
}

trap rollback ERR

# --- Step 0: Cluster Detection and Warning ---
clustered=false
corosync_conf="/etc/pve/corosync.conf"
if [ -f "$corosync_conf" ]; then
    clustered=true
    echo "WARNING: This node appears to be part of a Proxmox cluster!"
    echo "Renaming a clustered Proxmox node can cause cluster instability."
    echo "It is recommended to remove the node from the cluster, perform the rename, and re-join."
    echo "Proceeding may break your cluster configuration."
    read -p "Are you sure you want to proceed? (yes/NO): " confirm_cluster
    if [[ "$confirm_cluster" != "yes" ]]; then
        echo "Aborting script as requested."
        exit 1
    fi
fi

# --- Step 1: User Input ---
echo "Current node directories:"
ls /etc/pve/nodes/
echo ""

read -p "Enter previous hostname (the one with your VMs): " old_hostname
read -p "Enter new hostname: " new_hostname

# Basic validation
if [[ -z "$old_hostname" || -z "$new_hostname" ]]; then
    echo "Error: Hostnames cannot be empty"
    exit 1
fi
if ! [[ -d "/etc/pve/nodes/$old_hostname" ]]; then
    echo "Error: Source node directory not found"
    exit 1
fi

# --- Step 2: Backup Critical Data (for Rollback) ---
cp -a /etc/pve/nodes "$backup_dir/nodes"
cp /etc/hosts "$backup_dir/hosts"
[ -f /etc/pve/storage.cfg ] && cp /etc/pve/storage.cfg "$backup_dir/storage.cfg"
[ -f "$corosync_conf" ] && cp "$corosync_conf" "$backup_dir/corosync.conf"
[ -d "/etc/pve/nodes/$old_hostname/qemu-server" ] && cp -a "/etc/pve/nodes/$old_hostname/qemu-server" "$backup_dir/qemu-server"
[ -d "/etc/pve/nodes/$old_hostname/lxc" ] && cp -a "/etc/pve/nodes/$old_hostname/lxc" "$backup_dir/lxc"
for rrd_dir in node storage vm; do
    base_path="/var/lib/rrdcached/db/pve2-$rrd_dir"
    src="$base_path/$old_hostname"
    if [ -d "$src" ]; then
        cp -a "$src" "$backup_dir/pve2-$rrd_dir-$old_hostname"
    fi
done

# --- Step 3: Stop Services ---
systemctl stop pveproxy pve-cluster pvedaemon pvestatd
killall pmxcfs 2>/dev/null
sleep 2

# --- Step 4: Save VM and Container Configurations ---
temp_dir=$(mktemp -d)
trap "rm -rf $temp_dir" EXIT
cp -r "/etc/pve/nodes/$old_hostname/qemu-server" "$temp_dir/" 2>/dev/null || true
cp -r "/etc/pve/nodes/$old_hostname/lxc" "$temp_dir/" 2>/dev/null || true

# --- Step 5: Update Hostname ---
hostnamectl set-hostname "$new_hostname"
cp /etc/hosts /etc/hosts.bak
ip_address=$(ip -4 addr show | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | grep -v '127.0.0.1' | head -n 1)
sed -i "s/\b$old_hostname\b/$new_hostname/g" /etc/hosts
if ! grep -q "$ip_address" /etc/hosts; then
    echo "$ip_address $new_hostname $new_hostname.localdomain" >> /etc/hosts
fi

# --- Step 6: Edit corosync.conf if clustered ---
if [ "$clustered" = true ]; then
    sed -i "s/\b$old_hostname\b/$new_hostname/g" "$corosync_conf"
    current_version=$(grep -E '^\s*version:' "$corosync_conf" | awk '{print $2}')
    if [[ -n "$current_version" ]]; then
        new_version=$((current_version + 1))
        sed -i "s/^\(\s*version:\s*\)$current_version/\1$new_version/" "$corosync_conf"
        echo "corosync.conf version updated from $current_version to $new_version"
    else
        echo "WARNING: Could not find version line in corosync.conf. Please update it manually if needed."
    fi
fi

# --- Step 7: Copy rrdcached data directories to new hostname and remove old ones ---
for rrd_dir in node storage vm; do
    base_path="/var/lib/rrdcached/db/pve2-$rrd_dir"
    src="$base_path/$old_hostname"
    dst="$base_path/$new_hostname"
    if [ -d "$src" ]; then
        mkdir -p "$dst"
        cp -a "$src/." "$dst/"
        rm -rf "$src"
    fi
done

# --- Step 8: Clean Old Node Configuration ---
rm -rf "/etc/pve/nodes/$old_hostname"

# --- Step 9: Restore VM and Container Configurations ---
mkdir -p "/etc/pve/nodes/$new_hostname/qemu-server"
mkdir -p "/etc/pve/nodes/$new_hostname/lxc"
cp -r "$temp_dir/qemu-server/"* "/etc/pve/nodes/$new_hostname/qemu-server/" 2>/dev/null || true
cp -r "$temp_dir/lxc/"* "/etc/pve/nodes/$new_hostname/lxc/" 2>/dev/null || true

# --- Step 10: Restore Storage Configuration ---
if [ -f "$backup_dir/storage.cfg" ]; then
    cp "$backup_dir/storage.cfg" /etc/pve/
fi

# --- Step 11: Start Services ---
systemctl start pve-cluster
sleep 3
systemctl start pveproxy pvedaemon pvestatd

# --- Step 12: Cleanup and Success ---
trap - ERR
rm -rf "$backup_dir"
echo -e "\nFinal Verification:"
echo "Hostname: $(hostname)"
ls -l /etc/pve/nodes/
[ -f /etc/pve/storage.cfg ] && cat /etc/pve/storage.cfg || echo "No storage configuration found."
if [ "$clustered" = true ]; then
    echo "!!! WARNING: Clustered node detected. corosync.conf has been updated, but manual cluster validation and possibly service restarts are required."
fi
echo "Done!"
