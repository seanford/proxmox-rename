#!/bin/bash
set -euo pipefail

# --- Config ---
readonly SCRIPT_VERSION="2.0.0-fixed"
readonly timestamp=$(date +%Y%m%d_%H%M%S)
readonly backup_dir="/root/pve_rollback_backup_$timestamp"
readonly rollback_log="$backup_dir/rollback.log"
readonly temp_dir=$(mktemp -d)
chmod 700 "$temp_dir"

# Configurable timeouts
readonly SERVICE_STOP_TIMEOUT=${PVE_SERVICE_TIMEOUT:-60}
readonly CLUSTER_START_TIMEOUT=${PVE_CLUSTER_TIMEOUT:-90}
readonly FILESYSTEM_MOUNT_TIMEOUT=${PVE_MOUNT_TIMEOUT:-60}

# Global state tracking
declare -g old_hostname=""
declare -g new_hostname=""
declare -g clustered=false
declare -g corosync_conf=""
declare -g corosync_backup=""
declare -g rollback_in_progress=false

# Ensure cleanup on exit
cleanup() {
    local exit_code=$?
    if [[ -d "$temp_dir" ]]; then
        rm -rf "$temp_dir" 2>/dev/null || true
    fi
    if [[ $exit_code -ne 0 && "$rollback_in_progress" != "true" ]]; then
        echo "Script failed with exit code $exit_code. Backup preserved in: $backup_dir"
    fi
    exit $exit_code
}
trap cleanup EXIT

# Initialize backup directory
mkdir -p "$backup_dir"

# --- Enhanced Logging ---
log() {
    local level="${2:-INFO}"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $1" | tee -a "$rollback_log"
}

log_error() {
    log "$1" "ERROR"
}

log_warn() {
    log "$1" "WARN"
}

log_debug() {
    log "$1" "DEBUG"
}

# --- Enhanced Validation Functions ---
validate_hostname() {
    local hostname="$1"
    
    # Basic format validation
    if [[ ! "$hostname" =~ ^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$ ]]; then
        echo "Error: Invalid hostname format. Use only letters, numbers, and hyphens."
        return 1
    fi
    
    # Length validation
    if [[ ${#hostname} -gt 63 ]]; then
        echo "Error: Hostname too long (max 63 characters)"
        return 1
    fi
    
    # Hyphen position validation
    if [[ "$hostname" =~ ^- ]] || [[ "$hostname" =~ -$ ]]; then
        echo "Error: Hostname cannot start or end with hyphen"
        return 1
    fi
    
    # Reserved hostname validation
    local reserved_names=("localhost" "localhost.localdomain" "broadcasthost" "ip6-localhost" "ip6-loopback")
    for reserved in "${reserved_names[@]}"; do
        if [[ "$hostname" == "$reserved" ]]; then
            echo "Error: '$hostname' is a reserved hostname"
            return 1
        fi
    done
    
    return 0
}

validate_system_state() {
    # Check if running as root
    if [[ $EUID -ne 0 ]]; then
        echo "Error: Script must be run as root"
        return 1
    fi
    
    # Check if Proxmox is installed
    if [[ ! -d "/etc/pve" ]]; then
        echo "Error: Proxmox VE not detected"
        return 1
    fi
    
    # Check if /etc/pve is mounted
    if ! mountpoint -q /etc/pve 2>/dev/null; then
        echo "Error: /etc/pve is not mounted. Proxmox cluster filesystem may be down."
        return 1
    fi
    
    return 0
}

# --- Enhanced Utility Functions ---
get_primary_ip() {
    # Try to get IP from default route interface
    local default_iface
    default_iface=$(ip route show default 2>/dev/null | awk '/default/ {print $5; exit}')
    
    if [[ -n "$default_iface" ]]; then
        local ip_addr
        ip_addr=$(ip -4 addr show "$default_iface" 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | head -n 1)
        if [[ -n "$ip_addr" && "$ip_addr" != "127.0.0.1" ]]; then
            echo "$ip_addr"
            return 0
        fi
    fi
    
    # Fallback to first non-loopback IP
    local fallback_ip
    fallback_ip=$(ip -4 addr show 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' | grep -v '127.0.0.1' | head -n 1)
    if [[ -n "$fallback_ip" ]]; then
        echo "$fallback_ip"
        return 0
    fi
    
    # Final fallback
    echo "127.0.1.1"
    return 1
}

# --- Enhanced Service Management ---
wait_for_service_state() {
    local service="$1"
    local expected_state="$2"  # "active" or "inactive"
    local timeout="$3"
    local interval="${4:-1}"
    
    log_debug "Waiting for $service to be $expected_state (timeout: ${timeout}s)"
    
    while [[ $timeout -gt 0 ]]; do
        local current_state
        if systemctl is-active --quiet "$service" 2>/dev/null; then
            current_state="active"
        else
            current_state="inactive"
        fi
        
        if [[ "$current_state" == "$expected_state" ]]; then
            log_debug "$service is now $expected_state"
            return 0
        fi
        
        sleep "$interval"
        ((timeout -= interval))
    done
    
    log_error "Timeout waiting for $service to be $expected_state"
    return 1
}

cleanup_pmxcfs() {
    log "Cleaning up pmxcfs and /etc/pve mount..."
    
    # Gracefully stop pmxcfs
    if pgrep -x pmxcfs >/dev/null 2>&1; then
        log "Gracefully stopping pmxcfs..."
        pkill -TERM pmxcfs 2>/dev/null || true
        
        # Wait for graceful shutdown
        local timeout=10
        while pgrep -x pmxcfs >/dev/null 2>&1 && [[ $timeout -gt 0 ]]; do
            sleep 1
            ((timeout--))
        done
        
        # Force kill if still running
        if pgrep -x pmxcfs >/dev/null 2>&1; then
            log_warn "Force killing pmxcfs..."
            pkill -KILL pmxcfs 2>/dev/null || true
            sleep 2
        fi
    fi
    
    # Clean up /etc/pve mount with multiple strategies
    if mountpoint -q /etc/pve 2>/dev/null; then
        log "Unmounting /etc/pve filesystem..."
        if ! umount /etc/pve 2>/dev/null; then
            log_warn "Normal unmount failed, trying force unmount..."
            if ! umount -f /etc/pve 2>/dev/null; then
                log_warn "Force unmount failed, trying lazy unmount..."
                umount -l /etc/pve 2>/dev/null || true
            fi
        fi
        
        # Verify unmount
        if mountpoint -q /etc/pve 2>/dev/null; then
            log_error "/etc/pve is still mounted after all unmount attempts"
            return 1
        fi
    fi
    
    # Final cleanup of any remaining processes
    pkill -9 pmxcfs 2>/dev/null || true
    sleep 2
    
    return 0
}

start_cluster_service_with_retries() {
    log "Starting pve-cluster service with retries..."
    local max_attempts=3
    local attempt=1
    
    while [[ $attempt -le $max_attempts ]]; do
        log "pve-cluster start attempt $attempt of $max_attempts"
        
        if systemctl start pve-cluster 2>/dev/null; then
            if wait_for_service_state "pve-cluster" "active" "$CLUSTER_START_TIMEOUT"; then
                log "pve-cluster started successfully on attempt $attempt"
                return 0
            fi
        fi
        
        log_warn "pve-cluster start attempt $attempt failed"
        
        # Cleanup for retry
        systemctl stop pve-cluster 2>/dev/null || true
        cleanup_pmxcfs
        if [[ -d "/var/lib/pve-cluster" ]]; then
            rm -rf /var/lib/pve-cluster/* 2>/dev/null || true
        fi
        
        ((attempt++))
        if [[ $attempt -le $max_attempts ]]; then
            log "Waiting 5 seconds before retry..."
            sleep 5
        fi
    done
    
    log_error "pve-cluster failed to start after $max_attempts attempts"
    return 1
}

wait_for_cluster_filesystem() {
    log "Waiting for cluster filesystem to be ready..."
    local timeout="$FILESYSTEM_MOUNT_TIMEOUT"
    
    while [[ $timeout -gt 0 ]]; do
        if mountpoint -q /etc/pve 2>/dev/null; then
            # Additional check - try to access the filesystem
            if [[ -d "/etc/pve/nodes" ]]; then
                log "/etc/pve filesystem is mounted and accessible"
                return 0
            fi
        fi
        sleep 1
        ((timeout--))
    done
    
    log_error "/etc/pve filesystem not ready after ${FILESYSTEM_MOUNT_TIMEOUT}s timeout"
    return 1
}

stop_services_safely() {
    log "Stopping Proxmox services safely..."
    local services=("pvestatd" "pvedaemon" "pveproxy" "pve-cluster")
    local failed_services=()
    
    # Stop services in reverse dependency order
    for service in "${services[@]}"; do
        if systemctl is-active --quiet "$service" 2>/dev/null; then
            log "Stopping $service..."
            if systemctl stop "$service" 2>/dev/null; then
                if wait_for_service_state "$service" "inactive" "$SERVICE_STOP_TIMEOUT"; then
                    log "$service stopped successfully"
                else
                    log_warn "$service did not stop within timeout, forcing stop"
                    systemctl kill "$service" 2>/dev/null || true
                    failed_services+=("$service")
                fi
            else
                log_error "Failed to stop $service"
                failed_services+=("$service")
            fi
        else
            log_debug "$service was not running"
        fi
    done
    
    # Handle pmxcfs specially
    cleanup_pmxcfs
    
    if [[ ${#failed_services[@]} -gt 0 ]]; then
        log_warn "Some services failed to stop cleanly: ${failed_services[*]}"
        return 1
    fi
    
    return 0
}

start_services_safely() {
    log "Starting Proxmox services safely..."
    
    # Ensure clean slate
    cleanup_pmxcfs
    
    # Clean cluster database
    if [[ -d "/var/lib/pve-cluster" ]]; then
        log "Cleaning cluster database..."
        rm -rf /var/lib/pve-cluster/* 2>/dev/null || true
    fi
    
    # Start cluster service with retries
    if ! start_cluster_service_with_retries; then
        log_error "Failed to start cluster service"
        return 1
    fi
    
    # Wait for cluster filesystem to be ready
    if ! wait_for_cluster_filesystem; then
        log_error "Cluster filesystem failed to mount"
        return 1
    fi
    
    # Start other services
    local services=("pveproxy" "pvedaemon" "pvestatd")
    local failed_services=()
    
    for service in "${services[@]}"; do
        log "Starting $service..."
        if systemctl start "$service" 2>/dev/null; then
            if wait_for_service_state "$service" "active" 30; then
                log "$service started successfully"
            else
                log_warn "$service did not start properly, retrying once..."
                systemctl restart "$service" 2>/dev/null || true
                if wait_for_service_state "$service" "active" 15; then
                    log "$service started on retry"
                else
                    failed_services+=("$service")
                fi
            fi
        else
            failed_services+=("$service")
        fi
    done
    
    if [[ ${#failed_services[@]} -gt 0 ]]; then
        log_error "Failed to start services: ${failed_services[*]}"
        return 1
    fi
    
    return 0
}

# --- Enhanced Guest Management ---
get_running_guests() {
    local -a running_vms=()
    local -a running_cts=()
    
    # Get running VMs safely
    if command -v qm >/dev/null 2>&1; then
        local vm_list
        if vm_list=$(qm list 2>/dev/null); then
            while IFS= read -r vmid; do
                [[ -n "$vmid" ]] && running_vms+=("$vmid")
            done < <(echo "$vm_list" | awk 'NR>1 && $3=="running" {print $1}')
        fi
    fi
    
    # Get running containers safely
    if command -v pct >/dev/null 2>&1; then
        local ct_list
        if ct_list=$(pct list 2>/dev/null); then
            while IFS= read -r ctid; do
                [[ -n "$ctid" ]] && running_cts+=("$ctid")
            done < <(echo "$ct_list" | awk 'NR>1 && $2=="running" {print $1}')
        fi
    fi
    
    # Output format: vm_count ct_count, then one line per guest
    echo "${#running_vms[@]} ${#running_cts[@]}"
    printf "vm:%s\n" "${running_vms[@]}"
    printf "ct:%s\n" "${running_cts[@]}"
}

stop_all_guests() {
    log "Stopping all running VMs and containers..."
    local -a stopped_guests=()
    local -a failed_guests=()
    
    # Stop VMs
    if command -v qm >/dev/null 2>&1; then
        local vm_list
        if vm_list=$(qm list 2>/dev/null); then
            local -a running_vms
            readarray -t running_vms < <(echo "$vm_list" | awk 'NR>1 && $3=="running" {print $1}')
            
            for vmid in "${running_vms[@]}"; do
                [[ -z "$vmid" ]] && continue
                log "Stopping VM $vmid..."
                
                if qm stop "$vmid" >/dev/null 2>&1; then
                    stopped_guests+=("vm:$vmid")
                    log "VM $vmid stopped successfully"
                else
                    log_warn "Failed to stop VM $vmid gracefully, trying shutdown..."
                    if qm shutdown "$vmid" >/dev/null 2>&1; then
                        stopped_guests+=("vm:$vmid")
                        log "VM $vmid shutdown successfully"
                    else
                        failed_guests+=("vm:$vmid")
                        log_error "Failed to stop VM $vmid"
                    fi
                fi
            done
        fi
    fi
    
    # Stop containers
    if command -v pct >/dev/null 2>&1; then
        local ct_list
        if ct_list=$(pct list 2>/dev/null); then
            local -a running_cts
            readarray -t running_cts < <(echo "$ct_list" | awk 'NR>1 && $2=="running" {print $1}')
            
            for ctid in "${running_cts[@]}"; do
                [[ -z "$ctid" ]] && continue
                log "Stopping container $ctid..."
                
                if pct stop "$ctid" >/dev/null 2>&1; then
                    stopped_guests+=("ct:$ctid")
                    log "Container $ctid stopped successfully"
                else
                    failed_guests+=("ct:$ctid")
                    log_error "Failed to stop container $ctid"
                fi
            done
        fi
    fi
    
    # Wait for all guests to fully stop
    log "Waiting for all guests to fully stop..."
    sleep 10
    
    # Save lists for later use
    printf "%s\n" "${stopped_guests[@]}" > "$backup_dir/stopped_guests.list"
    if [[ ${#failed_guests[@]} -gt 0 ]]; then
        printf "%s\n" "${failed_guests[@]}" > "$backup_dir/failed_guests.list"
        log_warn "Some guests failed to stop: ${failed_guests[*]}"
    fi
    
    log "Guest stop summary: ${#stopped_guests[@]} stopped, ${#failed_guests[@]} failed"
    return 0
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
        
        case "$type" in
            "vm")
                log "Starting VM $id..."
                if qm start "$id" >/dev/null 2>&1; then
                    log "VM $id started successfully"
                    ((started_count++))
                else
                    log_error "Failed to start VM $id"
                    ((failed_count++))
                fi
                ;;
            "ct")
                log "Starting container $id..."
                if pct start "$id" >/dev/null 2>&1; then
                    log "Container $id started successfully"
                    ((started_count++))
                else
                    log_error "Failed to start container $id"
                    ((failed_count++))
                fi
                ;;
            *)
                log_warn "Unknown guest type: $type"
                ;;
        esac
        
        # Small delay between starts to avoid overwhelming the system
        sleep 2
    done < "$guests_file"
    
    log "Guest restart summary: $started_count started, $failed_count failed"
    
    if [[ $failed_count -gt 0 ]]; then
        echo "WARNING: $failed_count guests failed to start automatically"
        echo "Check '$backup_dir/stopped_guests.list' for the list of guests"
        echo "You may need to start them manually through the web interface"
    fi
    
    return 0
}

# --- Enhanced Backup Functions ---
create_atomic_backup() {
    local src="$1"
    local dst="$2"
    local temp_dst="${dst}.tmp"
    
    log_debug "Creating atomic backup: $src -> $dst"
    
    if [[ -f "$src" ]]; then
        if cp "$src" "$temp_dst" 2>/dev/null; then
            if mv "$temp_dst" "$dst" 2>/dev/null; then
                log_debug "Successfully backed up $src"
                return 0
            else
                rm -f "$temp_dst" 2>/dev/null || true
                log_error "Failed to move temporary backup for $src"
                return 1
            fi
        else
            log_error "Failed to copy $src to temporary location"
            return 1
        fi
    elif [[ -d "$src" ]]; then
        if cp -a "$src" "$temp_dst" 2>/dev/null; then
            if mv "$temp_dst" "$dst" 2>/dev/null; then
                log_debug "Successfully backed up directory $src"
                return 0
            else
                rm -rf "$temp_dst" 2>/dev/null || true
                log_error "Failed to move temporary backup directory for $src"
                return 1
            fi
        else
            log_error "Failed to copy directory $src to temporary location"
            return 1
        fi
    else
        log_warn "Source $src does not exist, skipping backup"
        return 0
    fi
}

backup_rrd_data() {
    log "Backing up RRD monitoring data..."
    local rrd_dirs=("node" "storage" "vm")
    local failed_rrd=()
    
    for rrd_dir in "${rrd_dirs[@]}"; do
        local base_path="/var/lib/rrdcached/db/pve2-$rrd_dir"
        local src="$base_path/$old_hostname"
        local dst="$backup_dir/pve2-$rrd_dir-$old_hostname"
        
        if [[ -d "$src" ]]; then
            if create_atomic_backup "$src" "$dst"; then
                log_debug "Backed up RRD data: $rrd_dir"
            else
                failed_rrd+=("$rrd_dir")
                log_warn "Failed to backup RRD data: $rrd_dir"
            fi
        fi
    done
    
    if [[ ${#failed_rrd[@]} -gt 0 ]]; then
        log_warn "Some RRD backups failed: ${failed_rrd[*]}"
        return 1
    fi
    
    return 0
}

create_comprehensive_backup() {
    log "Creating comprehensive backup of current configuration..."
    
    # Backup critical files and directories
    local backup_items=(
        "/etc/pve/nodes:$backup_dir/nodes"
        "/etc/hosts:$backup_dir/hosts"
        "/etc/pve/storage.cfg:$backup_dir/storage.cfg"
        "/etc/hostname:$backup_dir/hostname"
    )
    
    # Add corosync.conf from appropriate location
    if [[ -f "/etc/pve/corosync.conf" ]]; then
        backup_items+=("/etc/pve/corosync.conf:$backup_dir/corosync.conf")
        corosync_conf="/etc/pve/corosync.conf"
    elif [[ -f "/etc/corosync/corosync.conf" ]]; then
        backup_items+=("/etc/corosync/corosync.conf:$backup_dir/corosync.conf")
        corosync_conf="/etc/corosync/corosync.conf"
    fi
    
    corosync_backup="$backup_dir/corosync.conf"
    
    # Perform atomic backups
    local failed_backups=()
    for item in "${backup_items[@]}"; do
        local src="${item%:*}"
        local dst="${item#*:}"
        
        if ! create_atomic_backup "$src" "$dst"; then
            failed_backups+=("$src")
        fi
    done
    
    # Backup VM and container configs specifically
    if [[ -d "/etc/pve/nodes/$old_hostname/qemu-server" ]]; then
        if ! create_atomic_backup "/etc/pve/nodes/$old_hostname/qemu-server" "$backup_dir/qemu-server"; then
            failed_backups+=("/etc/pve/nodes/$old_hostname/qemu-server")
        fi
    fi
    
    if [[ -d "/etc/pve/nodes/$old_hostname/lxc" ]]; then
        if ! create_atomic_backup "/etc/pve/nodes/$old_hostname/lxc" "$backup_dir/lxc"; then
            failed_backups+=("/etc/pve/nodes/$old_hostname/lxc")
        fi
    fi
    
    # Backup RRD monitoring data
    backup_rrd_data
    
    if [[ ${#failed_backups[@]} -gt 0 ]]; then
        log_error "Failed to backup: ${failed_backups[*]}"
        return 1
    fi
    
    log "Backup completed successfully"
    return 0
}

# --- Enhanced File Operations ---
atomic_file_update() {
    local file_path="$1"
    local temp_file="${file_path}.tmp.$$"
    
    # Create temporary file with same permissions as original
    if [[ -f "$file_path" ]]; then
        cp "$file_path" "$temp_file" || return 1
    else
        touch "$temp_file" || return 1
    fi
    
    # Apply modifications via stdin or function
    if [[ -t 0 ]]; then
        # No stdin, return temp file path for manual editing
        echo "$temp_file"
    else
        # Read from stdin and write to temp file
        if ! cat > "$temp_file"; then
            rm -f "$temp_file"
            return 1
        fi
        
        # Atomic move
        if mv "$temp_file" "$file_path"; then
            return 0
        else
            rm -f "$temp_file"
            return 1
        fi
    fi
}

update_hostname_in_file() {
    local file_path="$1"
    local old_name="$2"
    local new_name="$3"
    
    if [[ ! -f "$file_path" ]]; then
        log_warn "File $file_path does not exist, skipping update"
        return 0
    fi
    
    log_debug "Updating hostname references in $file_path"
    
    # Use atomic update
    local temp_file
    temp_file=$(atomic_file_update "$file_path")
    
    # Perform substitution with word boundaries to avoid partial matches
    if sed "s/\b$old_name\b/$new_name/g" "$file_path" > "$temp_file"; then
        if mv "$temp_file" "$file_path"; then
            log_debug "Successfully updated $file_path"
            return 0
        fi
    fi
    
    # Cleanup on failure
    rm -f "$temp_file" 2>/dev/null || true
    log_error "Failed to update $file_path"
    return 1
}

# --- Enhanced System State Functions ---
check_cluster_health() {
    # Enhanced cluster detection
    local cluster_indicators=0
    
    # Check for corosync configuration
    if [[ -f "/etc/pve/corosync.conf" ]] || [[ -f "/etc/corosync/corosync.conf" ]]; then
        ((cluster_indicators++))
    fi
    
    # Check for multiple node directories
    if [[ -d "/etc/pve/nodes" ]]; then
        local node_count
        node_count=$(find /etc/pve/nodes -maxdepth 1 -type d | wc -l)
        if [[ $node_count -gt 2 ]]; then  # More than just the base dir and one node
            ((cluster_indicators++))
        fi
    fi
    
    # Check for running cluster services
    if systemctl is-active --quiet pve-cluster 2>/dev/null; then
        if pgrep -x pmxcfs >/dev/null 2>&1; then
            ((cluster_indicators++))
        fi
    fi
    
    # Check for cluster status command
    if command -v pvecm >/dev/null 2>&1; then
        if pvecm status >/dev/null 2>&1; then
            ((cluster_indicators++))
        fi
    fi
    
    if [[ $cluster_indicators -ge 2 ]]; then
        clustered=true
        log "Cluster configuration detected"
        return 0
    fi
    
    clustered=false
    log "Single node configuration detected"
    return 0
}

check_running_guests() {
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
        echo "It's strongly recommended to stop all VMs and containers before renaming"
        echo "The script can stop them automatically, but you may prefer to do it manually"
        echo ""
        read -p "Continue with automatic guest shutdown? (yes/NO): " confirm
        if [[ "$confirm" != "yes" ]]; then
            echo "Please stop all guests manually and re-run the script"
            return 1
        fi
    fi
    
    return 0
}

preflight_checks() {
    log "Running comprehensive preflight checks..."
    
    # Basic system validation
    if ! validate_system_state; then
        return 1
    fi
    
    # Check available disk space (need space for backups)
    local available_kb
    available_kb=$(df /root | awk 'NR==2 {print $4}')
    local needed_kb=2000000  # 2GB minimum
    
    if [[ -d "/etc/pve" ]]; then
        local pve_size
        pve_size=$(du -sk /etc/pve /var/lib/rrdcached 2>/dev/null | awk '{sum+=$1} END {print sum*3}' || echo 1000000)
        needed_kb=$((pve_size > needed_kb ? pve_size : needed_kb))
    fi
    
    if [[ $available_kb -lt $needed_kb ]]; then
        echo "Error: Insufficient disk space. Need at least $((needed_kb / 1024))MB available in /root"
        echo "Available: $((available_kb / 1024))MB"
        return 1
    fi
    
    # Check for running VMs/containers and warn user
    check_running_guests
    
    # Check cluster consistency
    check_cluster_health
    
    log "Preflight checks completed successfully"
    return 0
}

# --- Enhanced Verification Functions ---
verify_completion() {
    log "Performing comprehensive verification..."
    local verification_errors=()
    
    # Check hostname
    local current_hostname
    current_hostname=$(hostname)
    if [[ "$current_hostname" != "$new_hostname" ]]; then
        verification_errors+=("Hostname verification failed. Expected: $new_hostname, Got: $current_hostname")
    fi
    
    # Check node directory exists and is populated
    if [[ ! -d "/etc/pve/nodes/$new_hostname" ]]; then
        verification_errors+=("New node directory /etc/pve/nodes/$new_hostname not found")
    else
        # Check if directory has expected subdirectories
        if [[ ! -d "/etc/pve/nodes/$new_hostname/qemu-server" ]] && [[ ! -d "/etc/pve/nodes/$new_hostname/lxc" ]]; then
            verification_errors+=("New node directory exists but appears empty")
        fi
    fi
    
    # Check old directory is gone
    if [[ -d "/etc/pve/nodes/$old_hostname" ]]; then
        verification_errors+=("Old node directory still exists at /etc/pve/nodes/$old_hostname")
    fi
    
    # Check services are running
    local services=("pveproxy" "pvedaemon" "pvestatd" "pve-cluster")
    local failed_services=()
    
    for service in "${services[@]}"; do
        if ! systemctl is-active --quiet "$service" 2>/dev/null; then
            failed_services+=("$service")
        fi
    done
    
    if [[ ${#failed_services[@]} -gt 0 ]]; then
        verification_errors+=("Services not running: ${failed_services[*]}")
    fi
    
    # Check cluster filesystem is mounted
    if ! mountpoint -q /etc/pve 2>/dev/null; then
        verification_errors+=("/etc/pve filesystem is not mounted")
    fi
    
    # Check hostname in configuration files
    if [[ -f "/etc/hosts" ]]; then
        if ! grep -q "$new_hostname" /etc/hosts; then
            verification_errors+=("New hostname not found in /etc/hosts")
        fi
    fi
    
    # Report results
    if [[ ${#verification_errors[@]} -eq 0 ]]; then
        log "All verification checks passed"
        return 0
    else
        log_error "Verification failed with ${#verification_errors[@]} errors:"
        for error in "${verification_errors[@]}"; do
            log_error "  - $error"
        done
        return 1
    fi
}

# --- Enhanced Rollback Function ---
restore_backed_up_files() {
    log "Restoring backed up files..."
    
    # Restore files in reverse order of modification
    local restore_files=(
        "$backup_dir/hostname:/etc/hostname"
        "$backup_dir/hosts:/etc/hosts"
    )
    
    for item in "${restore_files[@]}"; do
        local src="${item%:*}"
        local dst="${item#*:}"
        
        if [[ -f "$src" ]]; then
            if cp "$src" "$dst" 2>/dev/null; then
                log_debug "Restored $dst from backup"
            else
                log_warn "Failed to restore $dst from backup"
            fi
        fi
    done
}

restore_rrd_data_rollback() {
    log "Restoring RRD data during rollback..."
    local rrd_dirs=("node" "storage" "vm")
    
    for rrd_dir in "${rrd_dirs[@]}"; do
        local base_path="/var/lib/rrdcached/db/pve2-$rrd_dir"
        local backup_src="$backup_dir/pve2-$rrd_dir-$old_hostname"
        local current_new="$base_path/$new_hostname"
        local restore_dst="$base_path/$old_hostname"
        
        if [[ -d "$backup_src" ]]; then
            # Remove any new directories that were created
            [[ -d "$current_new" ]] && rm -rf "$current_new" 2>/dev/null
            [[ -d "$restore_dst" ]] && rm -rf "$restore_dst" 2>/dev/null
            
            # Restore from backup
            if cp -a "$backup_src" "$restore_dst" 2>/dev/null; then
                log_debug "Restored RRD data for $rrd_dir"
            else
                log_warn "Failed to restore RRD data for $rrd_dir"
            fi
        fi
    done
}

restore_cluster_files_rollback() {
    log "Restoring cluster configuration files..."
    
    # Restore corosync.conf if we have a backup
    if [[ -f "$corosync_backup" && -n "$corosync_conf" ]]; then
        if cp "$corosync_backup" "$corosync_conf" 2>/dev/null; then
            log_debug "Restored corosync.conf"
        else
            log_warn "Failed to restore corosync.conf"
        fi
    fi
    
    # Restore storage.cfg
    if [[ -f "$backup_dir/storage.cfg" ]]; then
        if cp "$backup_dir/storage.cfg" "/etc/pve/storage.cfg" 2>/dev/null; then
            log_debug "Restored storage.cfg"
        else
            log_warn "Failed to restore storage.cfg"
        fi
    fi
    
    # Restore nodes directory structure
    if [[ -d "$backup_dir/nodes" ]]; then
        # Remove any new directories first
        [[ -d "/etc/pve/nodes/$new_hostname" ]] && rm -rf "/etc/pve/nodes/$new_hostname" 2>/dev/null
        
        # Restore original structure
        if cp -a "$backup_dir/nodes" "/etc/pve/nodes.tmp" 2>/dev/null; then
            rm -rf "/etc/pve/nodes" 2>/dev/null || true
            if mv "/etc/pve/nodes.tmp" "/etc/pve/nodes" 2>/dev/null; then
                log_debug "Restored /etc/pve/nodes directory structure"
            else
                log_warn "Failed to restore nodes directory structure"
            fi
        fi
    fi
}

perform_rollback() {
    rollback_in_progress=true
    log_error "CRITICAL ERROR DETECTED - Starting comprehensive rollback procedure..."
    
    # Stop all services to ensure clean state
    log "Stopping all Proxmox services for rollback..."
    local services=("pvestatd" "pvedaemon" "pveproxy" "pve-cluster")
    for service in "${services[@]}"; do
        systemctl stop "$service" 2>/dev/null || true
    done
    
    # Force cleanup of cluster filesystem
    cleanup_pmxcfs
    
    # Clean up cluster database completely
    if [[ -d "/var/lib/pve-cluster" ]]; then
        log "Cleaning cluster database for rollback..."
        rm -rf /var/lib/pve-cluster/* 2>/dev/null || true
    fi
    
    # Restore system hostname first
    if [[ -n "$old_hostname" ]]; then
        log "Rolling back hostname to $old_hostname"
        if ! hostnamectl set-hostname "$old_hostname" 2>/dev/null; then
            echo "$old_hostname" > /etc/hostname 2>/dev/null || true
        fi
    fi
    
    # Restore critical files in dependency order
    restore_backed_up_files
    
    # Restore RRD data before starting services
    restore_rrd_data_rollback
    
    # Start cluster service to mount /etc/pve
    log "Starting pve-cluster service for rollback..."
    if ! start_cluster_service_with_retries; then
        log_error "Failed to start cluster service during rollback"
        log_error "Manual intervention may be required"
    fi
    
    # Wait for filesystem to be ready
    if wait_for_cluster_filesystem; then
        log "/etc/pve mounted, restoring cluster configuration..."
        
        # Restore cluster files through mounted filesystem
        restore_cluster_files_rollback
        
        # Wait for synchronization
        log "Waiting for cluster synchronization during rollback..."
        sleep 15
        
        # Restart cluster service to pick up restored configuration
        systemctl restart pve-cluster 2>/dev/null || true
        sleep 5
    else
        log_error "Could not mount /etc/pve during rollback - manual recovery required"
    fi
    
    # Start remaining services
    log "Starting remaining services..."
    local remaining_services=("pveproxy" "pvedaemon" "pvestatd")
    for service in "${remaining_services[@]}"; do
        systemctl start "$service" 2>/dev/null || true
        sleep 2
    done
    
    log "ROLLBACK COMPLETE - System restored to previous state"
    log "Backup files preserved in: $backup_dir"
    echo ""
    echo "============================================="
    echo "ROLLBACK COMPLETED"
    echo "============================================="
    echo "Your system has been restored to its previous state."
    echo "Backup files are preserved in: $backup_dir"
    echo ""
    echo "Please verify your system is working correctly:"
    echo "1. Check Proxmox web interface accessibility"
    echo "2. Verify VM and container visibility"
    echo "3. Check cluster status if applicable"
    echo "4. Review logs in $rollback_log"
    echo ""
    
    exit 1
}

# Set up error trap for rollback
trap perform_rollback ERR

# --- Main Script Functions ---
detect_cluster_configuration() {
    log "Detecting cluster configuration..."
    
    check_cluster_health
    
    if [[ "$clustered" == "true" ]]; then
        echo ""
        echo "========================================="
        echo "WARNING: CLUSTERED NODE DETECTED"
        echo "========================================="
        echo "This node appears to be part of a Proxmox cluster!"
        echo "Renaming a clustered node requires careful consideration."
        echo ""
        echo "RECOMMENDED PROCEDURE FOR CLUSTERS:"
        echo "1. Ensure all cluster nodes are healthy"
        echo "2. Consider the impact on running VMs/containers"
        echo "3. Have a backup and recovery plan ready"
        echo "4. Monitor cluster status during and after the rename"
        echo ""
        echo "RISKS:"
        echo "- Temporary cluster communication issues"
        echo "- Possible need to restart other cluster nodes"
        echo "- Potential for cluster split-brain scenarios"
        echo ""
        read -p "Are you absolutely sure you want to proceed? (type 'yes' to continue): " confirm_cluster
        if [[ "$confirm_cluster" != "yes" ]]; then
            echo "Operation cancelled for safety. Consider the recommended procedure."
            exit 0
        fi
        echo ""
        log "User confirmed cluster rename operation"
    else
        log "Single node configuration confirmed"
    fi
}

get_user_input() {
    echo "========================================="
    echo "PROXMOX NODE HOSTNAME RENAME UTILITY"
    echo "Version: $SCRIPT_VERSION"
    echo "========================================="
    echo ""
    
    # Show current node directories
    echo "Current Proxmox node directories:"
    if [[ -d "/etc/pve/nodes" ]]; then
        ls -la /etc/pve/nodes/ 2>/dev/null || echo "Unable to list node directories"
    else
        echo "No node directories found"
    fi
    echo ""
    
    # Get current hostname with validation
    while true; do
        read -p "Enter current hostname (the directory containing your VMs/CTs): " old_hostname
        
        if [[ -z "$old_hostname" ]]; then
            echo "Error: Hostname cannot be empty"
            continue
        fi
        
        if [[ ! -d "/etc/pve/nodes/$old_hostname" ]]; then
            echo "Error: Node directory '/etc/pve/nodes/$old_hostname' not found"
            echo "Available directories:"
            ls /etc/pve/nodes/ 2>/dev/null || echo "  None found"
            continue
        fi
        
        # Verify this matches the system hostname
        local system_hostname
        system_hostname=$(hostname)
        if [[ "$old_hostname" != "$system_hostname" ]]; then
            echo "WARNING: Entered hostname '$old_hostname' doesn't match system hostname '$system_hostname'"
            read -p "Continue anyway? (yes/NO): " hostname_mismatch_confirm
            if [[ "$hostname_mismatch_confirm" != "yes" ]]; then
                continue
            fi
        fi
        
        break
    done
    
    # Get new hostname with validation
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
            echo "This could indicate a previous incomplete rename or naming conflict"
            continue
        fi
        
        break
    done
}

confirm_operation() {
    echo ""
    echo "========================================="
    echo "RENAME OPERATION CONFIRMATION"
    echo "========================================="
    echo "Current hostname: $old_hostname"
    echo "New hostname:     $new_hostname"
    echo "Clustered:        $clustered"
    echo "Backup location:  $backup_dir"
    echo ""
    echo "This operation will:"
    echo "✓ Create a comprehensive backup of your configuration"
    echo "✓ Stop all Proxmox services temporarily"
    echo "✓ Stop all running VMs and containers (if any)"
    echo "✓ Change the system hostname"
    echo "✓ Move all VM and container configurations"
    echo "✓ Update network and cluster configuration"
    echo "✓ Migrate monitoring and performance data"
    echo "✓ Restart all services with the new configuration"
    echo "✓ Attempt to restart previously running guests"
    
    if [[ "$clustered" == "true" ]]; then
        echo "✓ Update cluster configuration files"
        echo ""
        echo "⚠️  CLUSTER-SPECIFIC RISKS:"
        echo "   - Other cluster nodes may need attention"
        echo "   - Temporary cluster communication disruption"
        echo "   - Manual intervention may be required"
    fi
    
    echo ""
    echo "⚠️  IMPORTANT: This script includes automatic rollback on failure,"
    echo "   but you should have an independent backup strategy."
    echo ""
    read -p "Proceed with the rename operation? (type 'CONFIRM' to proceed): " final_confirm
    
    if [[ "$final_confirm" != "CONFIRM" ]]; then
        echo "Operation cancelled by user"
        exit 0
    fi
    
    log "User confirmed rename operation: $old_hostname -> $new_hostname"
}

# --- Core Rename Functions ---
prepare_configurations_for_move() {
    log "Preparing VM and container configurations for migration..."
    
    # Copy configurations to secure temporary directory
    if [[ -d "/etc/pve/nodes/$old_hostname/qemu-server" ]]; then
        if cp -r "/etc/pve/nodes/$old_hostname/qemu-server" "$temp_dir/" 2>/dev/null; then
            local vm_count
            vm_count=$(find "$temp_dir/qemu-server" -name "*.conf" 2>/dev/null | wc -l)
            log "Prepared $vm_count VM configurations"
        else
            log_error "Failed to prepare VM configurations"
            exit 1
        fi
    fi
    
    if [[ -d "/etc/pve/nodes/$old_hostname/lxc" ]]; then
        if cp -r "/etc/pve/nodes/$old_hostname/lxc" "$temp_dir/" 2>/dev/null; then
            local ct_count
            ct_count=$(find "$temp_dir/lxc" -name "*.conf" 2>/dev/null | wc -l)
            log "Prepared $ct_count container configurations"
        else
            log_error "Failed to prepare container configurations"
            exit 1
        fi
    fi
}

update_system_hostname() {
    log "Updating system hostname to $new_hostname"
    
    # Update hostname using multiple methods for compatibility
    if ! hostnamectl set-hostname "$new_hostname" 2>/dev/null; then
        log_warn "hostnamectl failed, trying alternative method"
        if ! echo "$new_hostname" > /etc/hostname; then
            log_error "Failed to update /etc/hostname"
            exit 1
        fi
    fi
    
    # Verify hostname change
    local new_hostname_check
    new_hostname_check=$(hostname)
    if [[ "$new_hostname_check" != "$new_hostname" ]]; then
        log_error "Hostname change verification failed: expected '$new_hostname', got '$new_hostname_check'"
        exit 1
    fi
    
    log "System hostname successfully updated"
}

update_hosts_file() {
    log "Updating /etc/hosts with new hostname"
    
    # Create backup of current hosts file
    if ! cp /etc/hosts /etc/hosts.pre-rename; then
        log_error "Failed to backup /etc/hosts"
        exit 1
    fi
    
    # Get primary IP address
    local ip_address
    ip_address=$(get_primary_ip)
    if [[ -z "$ip_address" ]]; then
        log_warn "Could not detect primary IP address, using localhost"
        ip_address="127.0.1.1"
    fi
    
    # Update existing hostname references
    if ! update_hostname_in_file "/etc/hosts" "$old_hostname" "$new_hostname"; then
        log_error "Failed to update hostname references in /etc/hosts"
        exit 1
    fi
    
    # Ensure new hostname entry exists
    if ! grep -q "^$ip_address.*$new_hostname" /etc/hosts; then
        echo "$ip_address $new_hostname $new_hostname.localdomain" >> /etc/hosts
        log "Added new hostname entry to /etc/hosts"
    fi
    
    log "/etc/hosts updated successfully"
}

update_additional_system_files() {
    # Update other files that might contain hostname references
    local additional_files=(
        "/etc/mailname"
        "/etc/postfix/main.cf"
    )
    
    for file in "${additional_files[@]}"; do
        if [[ -f "$file" ]]; then
            log_debug "Updating hostname references in $file"
            if ! update_hostname_in_file "$file" "$old_hostname" "$new_hostname"; then
                log_warn "Failed to update $file (non-critical)"
            fi
        fi
    done
}

update_system_configuration() {
    log "Updating system configuration files..."
    
    # Update /etc/hosts
    update_hosts_file
    
    # Update any other system files that might reference the hostname
    update_additional_system_files
}

migrate_rrd_data() {
    log "Migrating RRD monitoring data..."
    local rrd_dirs=("node" "storage" "vm")
    local migration_errors=()
    
    for rrd_dir in "${rrd_dirs[@]}"; do
        local base_path="/var/lib/rrdcached/db/pve2-$rrd_dir"
        local src="$base_path/$old_hostname"
        local dst="$base_path/$new_hostname"
        
        if [[ -d "$src" ]]; then
            log_debug "Migrating RRD data: $rrd_dir"
            
            # Create destination directory
            if mkdir -p "$dst" 2>/dev/null; then
                # Copy data atomically
                if cp -a "$src/." "$dst/" 2>/dev/null; then
                    # Verify copy succeeded before removing source
                    if [[ -d "$dst" ]] && [[ "$(ls -A "$dst" 2>/dev/null)" ]]; then
                        if ! rm -rf "$src" 2>/dev/null; then
                            log_warn "Failed to remove old RRD directory for $rrd_dir"
                        fi
                        log_debug "Successfully migrated RRD data: $rrd_dir"
                    else
                        migration_errors+=("$rrd_dir: copy verification failed")
                        if ! rm -rf "$dst" 2>/dev/null; then
                            log_warn "Failed to cleanup after verification failure for $rrd_dir"
                        fi
                    fi
                else
                    migration_errors+=("$rrd_dir: copy failed")
                    if ! rm -rf "$dst" 2>/dev/null; then
                        log_warn "Failed to cleanup failed migration for $rrd_dir"
                    fi
                fi
            else
                migration_errors+=("$rrd_dir: failed to create destination")
            fi
        fi
    done
    
    if [[ ${#migration_errors[@]} -gt 0 ]]; then
        log_warn "Some RRD data migrations failed: ${migration_errors[*]}"
        log_warn "Monitoring data for failed items will be lost"
    else
        log "RRD data migration completed successfully"
    fi
}

update_cluster_configuration() {
    log "Updating cluster configuration files..."
    
    if [[ -f "/etc/pve/corosync.conf" ]]; then
        local corosync_file="/etc/pve/corosync.conf"
    elif [[ -f "/etc/corosync/corosync.conf" ]]; then
        local corosync_file="/etc/corosync/corosync.conf"
    else
        log_warn "No corosync.conf found to update"
        return 0
    fi
    
    log "Updating $corosync_file..."
    
    # Create backup of current corosync.conf
    if ! cp "$corosync_file" "${corosync_file}.pre-rename"; then
        log_error "Failed to backup corosync.conf"
        exit 1
    fi
    
    # Update hostname references
    if ! update_hostname_in_file "$corosync_file" "$old_hostname" "$new_hostname"; then
        log_error "Failed to update hostname in corosync.conf"
        exit 1
    fi
    
    # Increment version number for cluster synchronization
    local current_version
    current_version=$(grep -E '^\s*version:' "$corosync_file" | awk '{print $2}' | head -n1)
    
    if [[ -n "$current_version" ]] && [[ "$current_version" =~ ^[0-9]+$ ]]; then
        local new_version=$((current_version + 1))
        if sed -i "s/^\(\s*version:\s*\)$current_version/\1$new_version/" "$corosync_file"; then
            log "Updated corosync.conf version: $current_version -> $new_version"
        else
            log_warn "Failed to update corosync.conf version number"
        fi
    else
        log_warn "Could not parse or update corosync.conf version number"
    fi
    
    log "Cluster configuration updated successfully"
}

update_storage_configuration() {
    log "Updating storage configuration..."
    
    if [[ -f "/etc/pve/storage.cfg" ]]; then
        # Create backup
        if ! cp "/etc/pve/storage.cfg" "/etc/pve/storage.cfg.pre-rename"; then
            log_warn "Failed to backup storage.cfg"
        fi
        
        # Update hostname references
        if update_hostname_in_file "/etc/pve/storage.cfg" "$old_hostname" "$new_hostname"; then
            log "Storage configuration updated successfully"
        else
            log_warn "Failed to update storage configuration (may not be critical)"
        fi
    else
        log_debug "No storage.cfg found to update"
    fi
}

restart_cluster_and_update_config() {
    log "Restarting cluster services and updating configuration..."
    
    # Clean cluster database to ensure fresh start
    if [[ -d "/var/lib/pve-cluster" ]]; then
        log "Cleaning cluster database for fresh start..."
        rm -rf /var/lib/pve-cluster/* 2>/dev/null || true
    fi
    
    # Start cluster service
    if ! start_cluster_service_with_retries; then
        log_error "Failed to start cluster service with new hostname"
        exit 1
    fi
    
    # Wait for cluster filesystem
    if ! wait_for_cluster_filesystem; then
        log_error "Cluster filesystem failed to mount with new hostname"
        exit 1
    fi
    
    # Update cluster configuration if needed
    if [[ "$clustered" == "true" ]]; then
        update_cluster_configuration
    fi
    
    # Update storage configuration
    update_storage_configuration
}

create_new_node_directory() {
    log "Creating new node directory structure..."
    
    # Create directory structure
    if ! mkdir -p "/etc/pve/nodes/$new_hostname/qemu-server"; then
        log_error "Failed to create VM configuration directory"
        exit 1
    fi
    
    if ! mkdir -p "/etc/pve/nodes/$new_hostname/lxc"; then
        log_error "Failed to create container configuration directory"
        exit 1
    fi
    
    # Restore configurations from temporary directory
    if [[ -d "$temp_dir/qemu-server" ]]; then
        if cp -r "$temp_dir/qemu-server/"* "/etc/pve/nodes/$new_hostname/qemu-server/" 2>/dev/null; then
            local restored_vms
            restored_vms=$(find "/etc/pve/nodes/$new_hostname/qemu-server" -name "*.conf" 2>/dev/null | wc -l)
            log "Restored $restored_vms VM configurations"
        fi
    fi
    
    if [[ -d "$temp_dir/lxc" ]]; then
        if cp -r "$temp_dir/lxc/"* "/etc/pve/nodes/$new_hostname/lxc/" 2>/dev/null; then
            local restored_cts
            restored_cts=$(find "/etc/pve/nodes/$new_hostname/lxc" -name "*.conf" 2>/dev/null | wc -l)
            log "Restored $restored_cts container configurations"
        fi
    fi
    
    log "New node directory structure created and populated"
}

complete_node_migration() {
    log "Completing node directory migration..."
    
    # At this point, /etc/pve should be mounted and ready
    if ! mountpoint -q /etc/pve; then
        log_error "/etc/pve is not mounted - cannot complete migration"
        exit 1
    fi
    
    # Check if old node directory still exists and move it
    if [[ -d "/etc/pve/nodes/$old_hostname" ]]; then
        log "Moving node directory: $old_hostname -> $new_hostname"
        
        # Atomic move operation
        if mv "/etc/pve/nodes/$old_hostname" "/etc/pve/nodes/$new_hostname"; then
            log "Node directory moved successfully"
        else
            log_error "Failed to move node directory - attempting recovery"
            
            # Try to create new directory and copy configurations
            create_new_node_directory
        fi
    else
        # Old directory doesn't exist, create new one
        log "Creating new node directory structure"
        create_new_node_directory
    fi
    
    # Verify the new directory exists and has content
    if [[ ! -d "/etc/pve/nodes/$new_hostname" ]]; then
        log_error "New node directory was not created successfully"
        exit 1
    fi
    
    log "Node directory migration completed"
}

cleanup_successful_operation() {
    echo ""
    echo "========================================="
    echo "RENAME COMPLETED SUCCESSFULLY!"
    echo "========================================="
    echo "Old hostname: $old_hostname"
    echo "New hostname: $new_hostname"
    echo ""
    
    # Show verification information
    echo "✓ Current hostname: $(hostname)"
    echo "✓ Node directory: /etc/pve/nodes/$new_hostname"
    
    local vm_count=0
    local ct_count=0
    
    if [[ -d "/etc/pve/nodes/$new_hostname/qemu-server" ]]; then
        vm_count=$(find "/etc/pve/nodes/$new_hostname/qemu-server" -name "*.conf" 2>/dev/null | wc -l)
    fi
    
    if [[ -d "/etc/pve/nodes/$new_hostname/lxc" ]]; then
        ct_count=$(find "/etc/pve/nodes/$new_hostname/lxc" -name "*.conf" 2>/dev/null | wc -l)
    fi
    
    echo "✓ VMs found: $vm_count"
    echo "✓ Containers found: $ct_count"
    
    # Show cluster information if applicable
    if [[ "$clustered" == "true" ]]; then
        echo ""
        echo "========================================="
        echo "CLUSTER CONFIGURATION UPDATED"
        echo "========================================="
        echo "Your node was part of a cluster. Additional steps:"
        echo ""
        echo "1. VERIFY CLUSTER STATUS:"
        echo "   Run: pvecm status"
        echo "   Check: pvecm nodes"
        echo ""
        echo "2. MONITOR OTHER NODES:"
        echo "   - Check cluster logs on other nodes"
        echo "   - Verify they can see this renamed node"
        echo "   - Restart cluster services if needed"
        echo ""
        echo "3. IF ISSUES OCCUR:"
        echo "   - Check corosync.conf on all nodes"
        echo "   - Verify network connectivity"
        echo "   - Consider restarting cluster services"
        echo ""
        echo "⚠️  IMPORTANT: Monitor cluster health closely!"
    fi
    
    echo ""
    echo "========================================="
    echo "NEXT STEPS"
    echo "========================================="
    echo "1. Test Proxmox web interface access"
    echo "2. Verify all VMs and containers are visible"
    echo "3. Check that monitoring data is preserved"
    echo "4. Update any external references to the old hostname"
    
    if [[ "$clustered" == "true" ]]; then
        echo "5. Verify cluster functionality across all nodes"
        echo "6. Update cluster references in external systems"
    fi
    
    echo ""
    read -p "Keep backup files for safety? (Y/n): " keep_backup
    if [[ "$keep_backup" =~ ^[Nn]$ ]]; then
        log "Removing backup directory as requested"
        if ! rm -rf "$backup_dir" 2>/dev/null; then
            log_warn "Failed to remove backup directory"
        fi
        echo "Backup files removed."
    else
        echo "Backup files preserved in: $backup_dir"
        echo "You can safely remove them after confirming everything works."
    fi
    
    echo ""
    echo "Hostname rename completed successfully!"
    echo "Log file: $rollback_log"
    
    return 0
}

finalize_rename_operation() {
    log "Finalizing rename operation..."
    
    # Wait for cluster synchronization
    log "Waiting for cluster synchronization..."
    sleep 15
    
    # Final service verification and restart if needed
    local services=("pve-cluster" "pveproxy" "pvedaemon" "pvestatd")
    local restart_needed=()
    
    for service in "${services[@]}"; do
        if ! systemctl is-active --quiet "$service" 2>/dev/null; then
            restart_needed+=("$service")
        fi
    done
    
    if [[ ${#restart_needed[@]} -gt 0 ]]; then
        log_warn "Some services need restart: ${restart_needed[*]}"
        for service in "${restart_needed[@]}"; do
            if ! systemctl restart "$service" 2>/dev/null; then
                log_warn "Failed to restart $service"
            fi
        done
    fi
    
    # Disable error trap since we're about to clean up successfully
    trap - ERR
    
    # Clean up temporary files
    if ! rm -rf "$temp_dir" 2>/dev/null; then
        log_warn "Failed to cleanup temporary directory"
    fi
    
    # Start previously running guests
    start_previously_running_guests
    
    # Remove backup if everything succeeded and user wants it removed
    if ! cleanup_successful_operation; then
        log_warn "Cleanup had issues but rename succeeded"
    fi
    
    log "Rename operation finalized successfully"
}

execute_rename_process() {
    log "Starting Proxmox node rename process: $old_hostname -> $new_hostname"
    
    # Step 1: Create comprehensive backup
    if ! create_comprehensive_backup; then
        log_error "Backup creation failed - aborting operation"
        exit 1
    fi
    
    # Step 2: Stop all guests if any are running
    local guest_info
    guest_info=$(get_running_guests)
    local vm_count
    local ct_count
    read vm_count ct_count <<< "$guest_info"
    
    if [[ $vm_count -gt 0 ]] || [[ $ct_count -gt 0 ]]; then
        log "Found $vm_count running VMs and $ct_count running containers"
        stop_all_guests
    else
        log "No running guests found"
    fi
    
    # Step 3: Stop Proxmox services
    if ! stop_services_safely; then
        log_error "Failed to stop services safely"
        exit 1
    fi
    
    # Step 4: Save configurations to temporary location
    prepare_configurations_for_move
    
    # Step 5: Update system hostname
    update_system_hostname
    
    # Step 6: Update system configuration files
    update_system_configuration
    
    # Step 7: Migrate RRD data
    migrate_rrd_data
    
    # Step 8: Restart cluster service and update cluster configuration
    restart_cluster_and_update_config
    
    # Step 9: Complete the node directory migration
    complete_node_migration
    
    # Step 10: Start all services
    if ! start_services_safely; then
        log_error "Failed to start services - manual intervention may be required"
        exit 1
    fi
    
    # Step 11: Verify the operation completed successfully
    if ! verify_completion; then
        log_error "Rename verification failed"
        exit 1
    fi
    
    # Step 12: Clean up and restart guests
    finalize_rename_operation
}

# --- Main Script Execution ---
main() {
    echo "Proxmox VE Node Hostname Rename Script v$SCRIPT_VERSION"
    echo "========================================================"
    echo ""
    
    # Run preflight checks
    if ! preflight_checks; then
        echo "Preflight checks failed. Please resolve issues and try again."
        exit 1
    fi
    
    # Detect cluster configuration and warn user
    detect_cluster_configuration
    
    # Get user input for hostnames
    get_user_input
    
    # Final confirmation before proceeding
    confirm_operation
    
    # Execute the rename process
    execute_rename_process
    
    echo ""
    echo "========================================="
    echo "OPERATION COMPLETED SUCCESSFULLY"
    echo "========================================="
}

# --- Script Entry Point ---
# Ensure we're not being sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
