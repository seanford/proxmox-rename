#!/bin/bash

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

# --- Step 6: Clean Old Node Configuration ---
rm -rf "/etc/pve/nodes/$old_hostname"

# --- Step 7: Restore VM and Container Configurations ---
mkdir -p "/etc/pve/nodes/$new_hostname/qemu-server"
mkdir -p "/etc/pve/nodes/$new_hostname/lxc"
cp -r "$temp_dir/qemu-server/"* "/etc/pve/nodes/$new_hostname/qemu-server/" 2>/dev/null || true
cp -r "$temp_dir/lxc/"* "/etc/pve/nodes/$new_hostname/lxc/" 2>/dev/null || true

# --- Step 8: Restore Storage Configuration ---
if [ -f "$backup_dir/storage.cfg" ]; then
    cp "$backup_dir/storage.cfg" /etc/pve/
fi

# --- Step 9: Start Services ---
echo "Starting Proxmox services..."
systemctl start pve-cluster
sleep 3
systemctl start pveproxy pvedaemon pvestatd

# --- Step 10: Final Verification ---
echo -e "\nFinal Verification:"
echo "Hostname: $(hostname)"
ls -l /etc/pve/nodes/
[ -f /etc/pve/storage.cfg ] && cat /etc/pve/storage.cfg || echo "No storage configuration found."
echo "Done!"
