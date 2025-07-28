#!/bin/bash
set -e

# --- Config ---
timestamp=$(date +%Y%m%d_%H%M%S)
backup_dir="/root/pve_rollback_backup_$timestamp"
rollback_log="$backup_dir/rollback.log"
mkdir -p "$backup_dir"

# --- Utility Functions ---
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$rollback_log"
}

validate_hostname() {
    local hostname="$1"
    if [[ ! "$hostname" =~ ^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$ ]]; then
        echo "Error: Invalid hostname format. Use only letters, numbers, and hyphens."
        return 1
    fi
    if [[ ${#hostname} -gt 63 ]]; then
        echo "Error: Hostname too long (max 63 characters)"
        return 1
    fi
    if [[ "$hostname" =~ ^- ]] || [[ "$hostname" =~ -$ ]]; then
        echo "Error: Hostname cannot start or end with hyphen"
        return 1
    fi
}

get_primary_ip() {
    # Try to get IP from default route interface
    local default_iface=$(ip route | awk '/default/ {print $5; exit}')
    if [[ -n "$default_iface" ]]; then
        local ip=$(ip -4 addr show "$default_iface" | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -n 1)
        if [[ -n "$ip" ]]; then
            echo "$ip"
            return 0
        fi
    fi
    
    # Fallback to first non-loopback IP
    ip -4 addr show | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | grep -v '127.0.0.1' | head -n 1
}

stop_services_safely() {
    log "Stopping Proxmox services..."
    local services=("pvestatd" "pvedaemon" "pveproxy" "pve-cluster")
    
    for service in "${services[@]}"; do
        if systemctl is-active --quiet "$service"; then
            log "Stopping $service..."
            systemctl stop "$service"
            
            # Wait for service to actually stop
            local timeout=30
            while systemctl is-active --quiet "$service" && [ $timeout -gt 0 ]; do
                sleep 1
                ((timeout--))
            done
            
            if systemctl is-active --quiet "$service"; then
                log "ERROR: Failed to stop $service after 30 seconds"
                return 1
            fi
            log "$service stopped successfully"
        fi
    done
    
    # Gracefully stop pmxcfs and clean up mount
    if pgrep pmxcfs > /dev/null; then
        log "Stopping pmxcfs..."
        pkill -TERM pmxcfs
        sleep 5
        if pgrep pmxcfs > /dev/null; then
            log "Force killing pmxcfs..."
            pkill -KILL pmxcfs
            sleep 2
        fi
    fi
    
    # Clean up any stale /etc/pve mounts
    if mountpoint -q /etc/pve; then
        log "Unmounting /etc/pve filesystem..."
        umount /etc/pve || {
            log "Force unmounting /etc/pve..."
            umount -f /etc/pve 2>/dev/null || true
            umount -l /etc/pve 2>/dev/null || true
        }
    fi
    
    # Kill any remaining pmxcfs processes
    pkill -9 pmxcfs 2>/dev/null || true
    sleep 2
}

start_services_safely() {
    log "Starting Proxmox services..."
    
    # Ensure /etc/pve is not mounted before starting
    if mountpoint -q /etc/pve; then
        log "Cleaning up stale /etc/pve mount..."
        umount /etc/pve 2>/dev/null || umount -f /etc/pve 2>/dev/null || umount -l /etc/pve 2>/dev/null || true
    fi
    
    # Kill any remaining pmxcfs processes
    pkill -9 pmxcfs 2>/dev/null || true
    sleep 2
    
    # Start cluster service first with retries
    log "Starting pve-cluster service..."
    local cluster_attempts=0
    local max_attempts=3
    
    while [ $cluster_attempts -lt $max_attempts ]; do
        systemctl start pve-cluster
        sleep 5
        
        if systemctl is-active --quiet pve-cluster; then
            log "pve-cluster started successfully"
            break
        else
            ((cluster_attempts++))
            log "pve-cluster start attempt $cluster_attempts failed, retrying..."
            
            # Clean up and try again
            systemctl stop pve-cluster 2>/dev/null || true
            pkill -9 pmxcfs 2>/dev/null || true
            if mountpoint -q /etc/pve; then
                umount -f /etc/pve 2>/dev/null || umount -l /etc/pve 2>/dev/null || true
            fi
            sleep 3
            
            if [ $cluster_attempts -eq $max_attempts ]; then
                log "ERROR: pve-cluster failed to start after $max_attempts attempts"
                log "This may require manual intervention"
                return 1
            fi
        fi
    done
    
    # Wait for pmxcfs to be ready
    local timeout=30
    while [ $timeout -gt 0 ] && ! mountpoint -q /etc/pve; do
        sleep 1
        ((timeout--))
    done
    
    if ! mountpoint -q /etc/pve; then
        log "WARNING: /etc/pve is not mounted after starting pve-cluster"
        log "You may need to restart the node or manually fix the cluster filesystem"
    else
        log "/etc/pve filesystem is mounted and ready"
    fi
    
    # Start other services
    sleep 3
    local services=("pveproxy" "pvedaemon" "pvestatd")
    for service in "${services[@]}"; do
        log "Starting $service..."
        systemctl start "$service"
        sleep 2
        if systemctl is-active --quiet "$service"; then
            log "$service started successfully"
        else
            log "WARNING: $service may not have started properly"
            # Try once more
            sleep 2
            systemctl start "$service" 2>/dev/null || true
        fi
    done
}

get_running_guests() {
    local running_vms=()
    local running_cts=()
    
    # Get running VMs
    if command -v qm >/dev/null 2>&1; then
        while IFS= read -r line; do
            [[ -n "$line" ]] && running_vms+=("$line")
        done < <(qm list 2>/dev/null | awk 'NR>1 && $3=="running" {print $1}')
    fi
    
    # Get running containers
    if command -v pct >/dev/null 2>&1; then
        while IFS= read -r line; do
            [[ -n "$line" ]] && running_cts+=("$line")
        done < <(pct list 2>/dev/null | awk 'NR>1 && $2=="running" {print $1}')
    fi
    
    echo "${#running_vms[@]} ${#running_cts[@]}"
    for vm in "${running_vms[@]}"; do
        echo "vm:$vm"
    done
    for ct in "${running_cts[@]}"; do
        echo "ct:$ct"
    done
}

stop_all_guests() {
    log "Stopping all running VMs and containers..."
    local stopped_guests=()
    
    # Stop VMs
    if command -v qm >/dev/null 2>&1; then
        local running_vms=($(qm list 2>/dev/null | awk 'NR>1 && $3=="running" {print $1}'))
        for vmid in "${running_vms[@]}"; do
            log "Stopping VM $vmid..."
            if qm stop "$vmid" >/dev/null 2>&1; then
                stopped_guests+=("vm:$vmid")
                log "VM $vmid stopped successfully"
            else
                log "WARNING: Failed to stop VM $vmid gracefully, trying shutdown..."
                qm shutdown "$vmid" >/dev/null 2>&1 || true
                stopped_guests+=("vm:$vmid")
            fi
        done
    fi
    
    # Stop containers
    if command -v pct >/dev/null 2>&1; then
        local running_cts=($(pct list 2>/dev/null | awk 'NR>1 && $2=="running" {print $1}'))
        for ctid in "${running_cts[@]}"; do
            log "Stopping container $ctid..."
            if pct stop "$ctid" >/dev/null 2>&1; then
                stopped_guests+=("ct:$ctid")
                log "Container $ctid stopped successfully"
            else
                log "WARNING: Failed to stop container $ctid gracefully"
                stopped_guests+=("ct:$ctid")
            fi
        done
    fi
    
    # Wait for all guests to stop
    log "Waiting for all guests to fully stop..."
    sleep 10
    
    # Save list of stopped guests for later restart
    printf "%s\n" "${stopped_guests[@]}" > "$backup_dir/stopped_guests.list"
    log "Saved list of stopped guests: ${#stopped_guests[@]} total"
}

start_previously_running_guests() {
    local guests_file="$backup_dir/stopped_guests.list"
    
    if [[ ! -f "$guests_file" ]]; then
        log "No previously running guests to restart"
        return 0
    fi
    
    log "Starting previously running guests..."
    local started_count=0
    local failed_count=0
    
    while IFS= read -r guest; do
        [[ -z "$guest" ]] && continue
        
        local type="${guest%:*}"
        local id="${guest#*:}"
        
        if [[ "$type" == "vm" ]]; then
            log "Starting VM $id..."
            if qm start "$id" >/dev/null 2>&1; then
                log "VM $id started successfully"
                ((started_count++))
            else
                log "ERROR: Failed to start VM $id"
                ((failed_count++))
            fi
        elif [[ "$type" == "ct" ]]; then
            log "Starting container $id..."
            if pct start "$id" >/dev/null 2>&1; then
                log "Container $id started successfully"
                ((started_count++))
            else
                log "ERROR: Failed to start container $id"
                ((failed_count++))
            fi
        fi
        
        # Small delay between starts
        sleep 2
    done < "$guests_file"
    
    log "Guest restart summary: $started_count started, $failed_count failed"
    
    if [[ $failed_count -gt 0 ]]; then
        echo "WARNING: $failed_count guests failed to start automatically"
        echo "You may need to start them manually through the web interface"
    fi
}
    log "Running preflight checks..."
    
    # Check if running as root
    if [[ $EUID -ne 0 ]]; then
        echo "Error: Script must be run as root"
        exit 1
    fi
    
    # Check if Proxmox is installed
    if [[ ! -d "/etc/pve" ]]; then
        echo "Error: Proxmox VE not detected"
        exit 1
    fi
    
    # Check available disk space (need space for backups)
    local available=$(df /root | awk 'NR==2 {print $4}')
    local needed=1000000  # 1GB minimum
    if [[ -d "/etc/pve" ]]; then
        needed=$(du -s /etc/pve /var/lib/rrdcached 2>/dev/null | awk '{sum+=$1} END {print sum*3}' || echo 1000000)
    fi
    
    if [[ $available -lt $needed ]]; then
        echo "Error: Insufficient disk space. Need at least $(( needed / 1024 ))MB available in /root"
        exit 1
    fi
    
    # Check for running VMs/containers
    local running_vms=0
    local running_cts=0
    
    if command -v qm >/dev/null 2>&1; then
        running_vms=$(qm list 2>/dev/null | awk 'NR>1 && $3=="running" {count++} END {print count+0}')
    fi
    
    if command -v pct >/dev/null 2>&1; then
        running_cts=$(pct list 2>/dev/null | awk 'NR>1 && $2=="running" {count++} END {print count+0}')
    fi
    
    if [[ $running_vms -gt 0 ]] || [[ $running_cts -gt 0 ]]; then
        echo "WARNING: Found $running_vms running VMs and $running_cts running containers"
        echo "It's recommended to stop all VMs and containers before renaming"
        read -p "Continue anyway? (yes/NO): " confirm
        if [[ "$confirm" != "yes" ]]; then
            echo "Aborting as requested"
            exit 1
        fi
    fi
    
    log "Preflight checks completed"
}

verify_completion() {
    log "Verifying rename completion..."
    
    # Check hostname
    local current_hostname=$(hostname)
    if [[ "$current_hostname" != "$new_hostname" ]]; then
        log "ERROR: Hostname verification failed. Expected: $new_hostname, Got: $current_hostname"
        return 1
    fi
    
    # Check node directory exists
    if [[ ! -d "/etc/pve/nodes/$new_hostname" ]]; then
        log "ERROR: New node directory /etc/pve/nodes/$new_hostname not found"
        return 1
    fi
    
    # Check old directory is gone
    if [[ -d "/etc/pve/nodes/$old_hostname" ]]; then
        log "WARNING: Old node directory still exists"
    fi
    
    # Check services are running
    local services=("pveproxy" "pvedaemon" "pvestatd" "pve-cluster")
    local failed_services=0
    
    for service in "${services[@]}"; do
        if ! systemctl is-active --quiet "$service"; then
            log "WARNING: Service $service is not running"
            ((failed_services++))
        fi
    done
    
    if [[ $failed_services -eq 0 ]]; then
        log "All services are running correctly"
    else
        log "WARNING: $failed_services services are not running properly"
    fi
    
    log "Verification completed"
    return 0
}

# --- Enhanced Rollback Function ---
rollback() {
    log "ERROR DETECTED - Starting rollback procedure..."
    
    # Stop services first (ignore errors)
    systemctl stop pvestatd pvedaemon pveproxy 2>/dev/null || true
    systemctl stop pve-cluster 2>/dev/null || true
    pkill -KILL pmxcfs 2>/dev/null || true
    
    # Clean up /etc/pve mount
    if mountpoint -q /etc/pve 2>/dev/null; then
        umount /etc/pve 2>/dev/null || umount -f /etc/pve 2>/dev/null || umount -l /etc/pve 2>/dev/null || true
    fi
    
    # Restore hostname first if we have the old one
    if [[ -n "$old_hostname" ]]; then
        log "Restoring hostname to $old_hostname"
        hostnamectl set-hostname "$old_hostname" 2>/dev/null || true
    fi
    
    # Restore files in reverse order of modification
    if [[ -f "$backup_dir/hosts" ]]; then
        cp "$backup_dir/hosts" /etc/hosts
        log "Restored /etc/hosts"
    fi
    
    if [[ -f "$corosync_backup" ]]; then
        # Determine correct location for corosync.conf
        if [[ -f "/etc/pve/corosync.conf" ]] || [[ "$corosync_conf" == "/etc/pve/corosync.conf" ]]; then
            cp "$corosync_backup" /etc/pve/corosync.conf
            log "Restored corosync.conf to /etc/pve/"
        elif [[ -f "/etc/corosync/corosync.conf" ]] || [[ "$corosync_conf" == "/etc/corosync/corosync.conf" ]]; then
            cp "$corosync_backup" /etc/corosync/corosync.conf
            log "Restored corosync.conf to /etc/corosync/"
        fi
    fi
    
    if [[ -f "$backup_dir/storage.cfg" ]]; then
        cp "$backup_dir/storage.cfg" /etc/pve/storage.cfg
        log "Restored storage.cfg"
    fi
    
    # Restore directory structures
    if [[ -d "$backup_dir/nodes" ]]; then
        rm -rf /etc/pve/nodes
        cp -a "$backup_dir/nodes" /etc/pve/nodes
        log "Restored /etc/pve/nodes"
    fi
    
    # Restore RRD data
    for rrd_dir in node storage vm; do
        base_path="/var/lib/rrdcached/db/pve2-$rrd_dir"
        if [[ -d "$backup_dir/pve2-$rrd_dir-$old_hostname" ]]; then
            if [[ -d "$base_path/$new_hostname" ]]; then
                rm -rf "$base_path/$new_hostname"
            fi
            mv "$backup_dir/pve2-$rrd_dir-$old_hostname" "$base_path/$old_hostname" 2>/dev/null || true
            log "Restored RRD data for $rrd_dir"
        fi
    done
    
    # Attempt to restart services
    log "Attempting to restart services..."
    
    # Clean up /etc/pve mount before starting
    if mountpoint -q /etc/pve 2>/dev/null; then
        umount /etc/pve 2>/dev/null || umount -f /etc/pve 2>/dev/null || umount -l /etc/pve 2>/dev/null || true
    fi
    
    systemctl start pve-cluster 2>/dev/null || true
    sleep 5
    systemctl start pveproxy pvedaemon pvestatd 2>/dev/null || true
    
    log "ROLLBACK COMPLETE - System restored to previous state"
    log "Backup files preserved in: $backup_dir"
    echo ""
    echo "ROLLBACK COMPLETED"
    echo "Your system has been restored to its previous state."
    echo "Backup files are preserved in: $backup_dir"
    echo "Please verify your system is working correctly."
    exit 1
}

# Set up error trap
trap rollback ERR

# --- Main Script Execution ---

# Run preflight checks
preflight_checks

# --- Step 0: Cluster Detection and Warning ---
clustered=false
corosync_conf="/etc/pve/corosync.conf"
corosync_backup="$backup_dir/corosync.conf"

# Check multiple indicators for clustering
if [[ -f "$corosync_conf" ]] || [[ -f "/etc/corosync/corosync.conf" ]] || [[ -d "/etc/pve/nodes" && $(ls -1 /etc/pve/nodes/ 2>/dev/null | wc -l) -gt 1 ]]; then
    clustered=true
    echo ""
    echo "========================================="
    echo "WARNING: CLUSTERED NODE DETECTED"
    echo "========================================="
    echo "This node appears to be part of a Proxmox cluster!"
    echo "Renaming a clustered node can cause cluster instability."
    echo ""
    echo "RECOMMENDED PROCEDURE:"
    echo "1. Remove this node from the cluster"
    echo "2. Perform the hostname rename"
    echo "3. Re-join the cluster with the new name"
    echo ""
    echo "Proceeding with this script may break your cluster!"
    echo ""
    read -p "Are you absolutely sure you want to proceed? (type 'yes' to continue): " confirm_cluster
    if [[ "$confirm_cluster" != "yes" ]]; then
        echo "Aborting script as requested."
        exit 1
    fi
    echo ""
fi

# --- Step 1: User Input ---
echo "Current Proxmox node directories:"
ls -la /etc/pve/nodes/ 2>/dev/null || echo "Unable to list node directories"
echo ""

while true; do
    read -p "Enter current hostname (the directory containing your VMs/CTs): " old_hostname
    if [[ -z "$old_hostname" ]]; then
        echo "Error: Hostname cannot be empty"
        continue
    fi
    if [[ ! -d "/etc/pve/nodes/$old_hostname" ]]; then
        echo "Error: Node directory '/etc/pve/nodes/$old_hostname' not found"
        echo "Available directories:"
        ls /etc/pve/nodes/ 2>/dev/null || echo "None found"
        continue
    fi
    break
done

while true; do
    read -p "Enter new hostname: " new_hostname
    if [[ -z "$new_hostname" ]]; then
        echo "Error: Hostname cannot be empty"
        continue
    fi
    if ! validate_hostname "$new_hostname"; then
        continue
    fi
    if [[ "$old_hostname" == "$new_hostname" ]]; then
        echo "Error: New hostname must be different from current hostname"
        continue
    fi
    if [[ -d "/etc/pve/nodes/$new_hostname" ]]; then
        echo "Error: Node directory for '$new_hostname' already exists"
        continue
    fi
    break
done

# Confirm the operation
echo ""
echo "========================================="
echo "RENAME CONFIRMATION"
echo "========================================="
echo "Old hostname: $old_hostname"
echo "New hostname: $new_hostname"
echo "Clustered: $clustered"
echo ""
echo "This will:"
echo "- Stop all Proxmox services temporarily"
echo "- Change system hostname"
echo "- Move all VM and container configurations"
echo "- Update network configuration"
echo "- Migrate monitoring data"
if [[ "$clustered" == "true" ]]; then
    echo "- Update cluster configuration"
fi
echo ""
read -p "Proceed with rename? (type 'yes' to confirm): " final_confirm
if [[ "$final_confirm" != "yes" ]]; then
    echo "Operation cancelled"
    exit 0
fi

log "Starting Proxmox node rename: $old_hostname -> $new_hostname"

# --- Step 2: Create Comprehensive Backup ---
log "Creating backup of current configuration..."

# Backup critical files and directories
cp -a /etc/pve/nodes "$backup_dir/nodes"
cp /etc/hosts "$backup_dir/hosts"

[[ -f /etc/pve/storage.cfg ]] && cp /etc/pve/storage.cfg "$backup_dir/storage.cfg"
# Backup corosync.conf if it exists (check both locations)
if [[ -f "$corosync_conf" ]]; then
    cp "$corosync_conf" "$corosync_backup"
    log "Backed up corosync.conf from /etc/pve/"
elif [[ -f "/etc/corosync/corosync.conf" ]]; then
    cp "/etc/corosync/corosync.conf" "$corosync_backup"
    corosync_conf="/etc/corosync/corosync.conf"  # Update path for later use
    log "Backed up corosync.conf from /etc/corosync/"
fi

# Backup VM and container configs specifically
if [[ -d "/etc/pve/nodes/$old_hostname/qemu-server" ]]; then
    cp -a "/etc/pve/nodes/$old_hostname/qemu-server" "$backup_dir/qemu-server"
fi

if [[ -d "/etc/pve/nodes/$old_hostname/lxc" ]]; then
    cp -a "/etc/pve/nodes/$old_hostname/lxc" "$backup_dir/lxc"
fi

# Backup RRD monitoring data
for rrd_dir in node storage vm; do
    base_path="/var/lib/rrdcached/db/pve2-$rrd_dir"
    src="$base_path/$old_hostname"
    if [[ -d "$src" ]]; then
        cp -a "$src" "$backup_dir/pve2-$rrd_dir-$old_hostname"
        log "Backed up RRD data: $rrd_dir"
    fi
done

log "Backup completed successfully"

# --- Step 3: Stop Services ---
stop_services_safely

# --- Step 4: Save VM and Container Configurations to Temp ---
log "Preparing VM and container configurations..."
temp_dir=$(mktemp -d)
trap "rm -rf $temp_dir; rollback" ERR

if [[ -d "/etc/pve/nodes/$old_hostname/qemu-server" ]]; then
    cp -r "/etc/pve/nodes/$old_hostname/qemu-server" "$temp_dir/" 2>/dev/null || true
    vm_count=$(ls -1 "/etc/pve/nodes/$old_hostname/qemu-server"/*.conf 2>/dev/null | wc -l)
    log "Found $vm_count VM configurations"
fi

if [[ -d "/etc/pve/nodes/$old_hostname/lxc" ]]; then
    cp -r "/etc/pve/nodes/$old_hostname/lxc" "$temp_dir/" 2>/dev/null || true
    ct_count=$(ls -1 "/etc/pve/nodes/$old_hostname/lxc"/*.conf 2>/dev/null | wc -l)
    log "Found $ct_count container configurations"
fi

# --- Step 5: Update Hostname ---
log "Updating system hostname to $new_hostname"
hostnamectl set-hostname "$new_hostname"

# Verify hostname change
new_hostname_check=$(hostname)
if [[ "$new_hostname_check" != "$new_hostname" ]]; then
    log "ERROR: Hostname change failed"
    exit 1
fi

# --- Step 6: Update /etc/hosts ---
log "Updating /etc/hosts"
cp /etc/hosts /etc/hosts.bak

# Get primary IP address
ip_address=$(get_primary_ip)
if [[ -z "$ip_address" ]]; then
    log "WARNING: Could not detect primary IP address"
    ip_address="127.0.1.1"  # Fallback
fi

# Update existing entries
sed -i "s/\b$old_hostname\b/$new_hostname/g" /etc/hosts

# Ensure new hostname entry exists
if ! grep -q "^$ip_address.*$new_hostname" /etc/hosts; then
    echo "$ip_address $new_hostname $new_hostname.localdomain" >> /etc/hosts
    log "Added new hostname entry to /etc/hosts"
fi

# --- Step 7: Update corosync.conf if clustered ---
if [[ "$clustered" == "true" ]] && [[ -f "$corosync_backup" ]]; then
    log "Updating cluster configuration..."
    
    # Work with the backed up copy first, then restore it
    temp_corosync=$(mktemp)
    cp "$corosync_backup" "$temp_corosync"
    
    # Update hostname references in temp file
    sed -i "s/\b$old_hostname\b/$new_hostname/g" "$temp_corosync"
    
    # Increment version number
    current_version=$(grep -E '^\s*version:' "$temp_corosync" | awk '{print $2}')
    if [[ -n "$current_version" ]] && [[ "$current_version" =~ ^[0-9]+$ ]]; then
        new_version=$((current_version + 1))
        sed -i "s/^\(\s*version:\s*\)$current_version/\1$new_version/" "$temp_corosync"
        log "Updated corosync.conf version: $current_version -> $new_version"
    else
        log "WARNING: Could not update corosync.conf version automatically"
    fi
    
    # Copy the updated file back to the correct location
    if [[ -f "/etc/pve/corosync.conf" ]] || [[ "$corosync_conf" == "/etc/pve/corosync.conf" ]]; then
        cp "$temp_corosync" "/etc/pve/corosync.conf"
        log "Updated /etc/pve/corosync.conf"
    elif [[ -f "/etc/corosync/corosync.conf" ]] || [[ "$corosync_conf" == "/etc/corosync/corosync.conf" ]]; then
        cp "$temp_corosync" "/etc/corosync/corosync.conf"
        log "Updated /etc/corosync/corosync.conf"
    fi
    
    rm -f "$temp_corosync"
elif [[ "$clustered" == "true" ]]; then
    log "WARNING: Cluster detected but no corosync.conf found to update"
fi

# --- Step 8: Migrate RRD Data ---
log "Migrating monitoring data..."
for rrd_dir in node storage vm; do
    base_path="/var/lib/rrdcached/db/pve2-$rrd_dir"
    src="$base_path/$old_hostname"
    dst="$base_path/$new_hostname"
    
    if [[ -d "$src" ]]; then
        mkdir -p "$dst"
        cp -a "$src/." "$dst/"
        rm -rf "$src"
        log "Migrated RRD data: $rrd_dir"
    fi
done

# --- Step 9: Update Node Directory Structure ---
log "Updating node directory structure..."

# Remove old node directory
rm -rf "/etc/pve/nodes/$old_hostname"

# Create new node directory structure
mkdir -p "/etc/pve/nodes/$new_hostname/qemu-server"
mkdir -p "/etc/pve/nodes/$new_hostname/lxc"

# --- Step 10: Restore VM and Container Configurations ---
log "Restoring VM and container configurations..."

if [[ -d "$temp_dir/qemu-server" ]]; then
    cp -r "$temp_dir/qemu-server/"* "/etc/pve/nodes/$new_hostname/qemu-server/" 2>/dev/null || true
    restored_vms=$(ls -1 "/etc/pve/nodes/$new_hostname/qemu-server"/*.conf 2>/dev/null | wc -l)
    log "Restored $restored_vms VM configurations"
fi

if [[ -d "$temp_dir/lxc" ]]; then
    cp -r "$temp_dir/lxc/"* "/etc/pve/nodes/$new_hostname/lxc/" 2>/dev/null || true
    restored_cts=$(ls -1 "/etc/pve/nodes/$new_hostname/lxc"/*.conf 2>/dev/null | wc -l)
    log "Restored $restored_cts container configurations"
fi

# --- Step 11: Update Storage Configuration ---
if [[ -f "/etc/pve/storage.cfg" ]]; then
    log "Updating storage configuration references..."
    # Update any hostname references in storage.cfg
    sed -i "s/\b$old_hostname\b/$new_hostname/g" /etc/pve/storage.cfg
fi

# --- Step 12: Start Services ---
start_services_safely

# --- Step 13: Verify and Cleanup ---
if verify_completion; then
    # Only cleanup backup if verification succeeds
    trap - ERR
    rm -rf "$backup_dir"
    rm -rf "$temp_dir"
    
    log "Proxmox node rename completed successfully!"
    
    echo ""
    echo "========================================="
    echo "RENAME COMPLETED SUCCESSFULLY"
    echo "========================================="
    echo "Old hostname: $old_hostname"
    echo "New hostname: $new_hostname"
    echo ""
    echo "Final verification:"
    echo "- Current hostname: $(hostname)"
    echo "- Node directory: /etc/pve/nodes/$new_hostname"
    echo "- VMs found: $(ls -1 /etc/pve/nodes/$new_hostname/qemu-server/*.conf 2>/dev/null | wc -l)"
    echo "- Containers found: $(ls -1 /etc/pve/nodes/$new_hostname/lxc/*.conf 2>/dev/null | wc -l)"
    
    if [[ "$clustered" == "true" ]]; then
        echo ""
        echo "========================================="
        echo "IMPORTANT: CLUSTER FOLLOW-UP REQUIRED"
        echo "========================================="
        echo "Your node was part of a cluster. You MUST:"
        echo "1. Restart this node to ensure all changes take effect"
        echo "2. Verify cluster status with: pvecm status"
        echo "3. Check cluster logs for any issues"
        echo "4. Restart cluster services on other nodes if needed"
        echo ""
        echo "If cluster issues persist, you may need to:"
        echo "- Remove and re-add this node to the cluster"
        echo "- Update cluster configuration on other nodes"
    fi
    
    # Start previously running guests
    start_previously_running_guests
    
    echo ""
    echo "Rename operation completed successfully!"
    
else
    log "Verification failed - leaving backup intact"
    echo ""
    echo "========================================="
    echo "VERIFICATION FAILED"
    echo "========================================="
    echo "The rename appears to have completed but verification failed."
    echo "Backup files are preserved in: $backup_dir"
    echo "Please check the system manually and review the log."
    echo ""
    echo "You may need to:"
    echo "1. Restart Proxmox services manually"
    echo "2. Check system logs for errors"
    echo "3. Verify VM and container accessibility"
    
    exit 1
fi
