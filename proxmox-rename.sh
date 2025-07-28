#!/bin/bash

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

# --- Step 2: Backup Critical Data ---
echo "Creating backup..."
backup_dir="/root/pve_backup_$(date +%Y%m%d_%H%M%S)"
mkdir -p "$backup_dir"
cp -r "/etc/pve/nodes" "$backup_dir/"
cp /etc/hosts "$backup_dir/"
[ -f /var/lib/pve-cluster/config.db ] && cp /var/lib/pve-cluster/config.db "$backup_dir/"
[ -f /etc/pve/storage.cfg ] && cp /etc/pve/storage.cfg "$backup_dir/"
if [ -f "$corosync_conf" ]; then
    cp "$corosync_conf" "$backup_dir/"
    cp "$corosync_conf" "$corosync_conf.bak"
    echo "Backed up $corosync_conf to $backup_dir/ and $corosync_conf.bak"
fi
echo "Backup created at: $backup_dir"

# --- Step 3: Stop Services ---
echo "Stopping Proxmox services..."
systemctl stop pveproxy pve-cluster pvedaemon pvestatd
killall pmxcfs 2>/dev/null
sleep 2

# --- Step 4: Save VM and Container Configurations ---
echo "Saving VM and container configurations..."
temp_dir=$(mktemp -d)
trap "rm -rf $temp_dir" EXIT
cp -r "/etc/pve/nodes/$old_hostname/qemu-server" "$temp_dir/" 2>/dev/null || true
cp -r "/etc/pve/nodes/$old_hostname/lxc" "$temp_dir/" 2>/dev/null || true

# --- Step 5: Update Hostname ---
echo "Updating hostname..."
hostnamectl set-hostname "$new_hostname"

cp /etc/hosts /etc/hosts.bak
ip_address=$(ip -4 addr show | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | grep -v '127.0.0.1' | head -n 1)
if [[ -z "$ip_address" ]]; then
    echo "Error: Could not determine IP address."
    exit 1
fi
sed -i "s/\b$old_hostname\b/$new_hostname/g" /etc/hosts
if ! grep -q "$ip_address" /etc/hosts; then
    echo "$ip_address $new_hostname $new_hostname.localdomain" >> /etc/hosts
fi

# --- Step 6: Edit corosync.conf if clustered ---
if [ "$clustered" = true ]; then
    echo "Editing $corosync_conf to update hostname and version..."

    # Replace old hostname with new hostname
    sed -i "s/\b$old_hostname\b/$new_hostname/g" "$corosync_conf"

    # Update version line: increment version number by 1
    current_version=$(grep -E '^\s*version:' "$corosync_conf" | awk '{print $2}')
    if [[ -n "$current_version" ]]; then
        new_version=$((current_version + 1))
        sed -i "s/^\(\s*version:\s*\)$current_version/\1$new_version/" "$corosync_conf"
        echo "corosync.conf version updated from $current_version to $new_version"
    else
        echo "WARNING: Could not find version line in corosync.conf. Please update it manually if needed."
    fi

    echo "corosync.conf updated. You may need to restart cluster services!"
fi

# --- Step 7: Copy rrdcached data directories to new hostname and remove old ones ---
for rrd_dir in node storage vm; do
    base_path="/var/lib/rrdcached/db/pve2-$rrd_dir"
    src="$base_path/$old_hostname"
    dst="$base_path/$new_hostname"
    if [ -d "$src" ]; then
        echo "Copying $src to $dst ..."
        mkdir -p "$dst"
        cp -a "$src/." "$dst/"
        rm -rf "$src"
        echo "Done copying and removing $src."
    else
        echo "Directory $src does not exist, skipping."
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
echo "Starting Proxmox services..."
systemctl start pve-cluster
sleep 3
systemctl start pveproxy pvedaemon pvestatd

# --- Step 12: Final Verification ---
echo -e "\nFinal Verification:"
echo "Hostname: $(hostname)"
ls -l /etc/pve/nodes/
[ -f /etc/pve/storage.cfg ] && cat /etc/pve/storage.cfg || echo "No storage configuration found."
if [ "$clustered" = true ]; then
    echo "!!! WARNING: Clustered node detected. corosync.conf has been updated, but manual cluster validation and possibly service restarts are required."
fi
echo "Done!"
