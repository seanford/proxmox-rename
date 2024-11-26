# Proxmox Node Rename Script

A script to rename a single, non-clustered Proxmox node while preserving all VMs, containers, and configurations.

## Prerequisites

- SSH access to the Proxmox server with root privileges
- Single node (non-clustered) Proxmox installation
- Backup of important data (script creates its own backups, but additional backups recommended)

## Installation

1. Save the script:
```bash
vi proxmox-rename.sh
```

2. Make executable:
```bash
chmod +x proxmox-rename.sh
```

## Usage

```bash
./proxmox-rename.sh
```

The script will:
1. Prompt for current hostname
2. Prompt for new hostname
3. Create backup of current configuration
4. Perform the rename process
5. Restart necessary services

## Features

- Creates timestamped backups in `/root/pve_backup_YYYYMMDD_HHMMSS/`
- Preserves VM and container configurations
- Handles storage configurations
- Updates hostname and system files
- Restarts required services

## Storage Support

- ✅ `local-lvm` (Tested and verified)
- ⚠️ `local` (Supported, untested)
- ⚠️ `local-zfs` (Supported, untested)

## Troubleshooting

### Service Issues
Check service status:
```bash
systemctl status pveproxy pve-cluster pvedaemon pvestatd
```

### Configuration Recovery
Restore from backup:
```bash
cp -r /root/pve_backup_<timestamp>/nodes/<old_hostname>/* /etc/pve/nodes/<new_hostname>/
```

### Verify VMs and Containers
```bash
qm list    # List VMs
pct list   # List containers
```

## Notes

- Always verify backups exist before making changes
- Refresh Proxmox web interface after rename
- Script intended for single-node setups only
- Test in non-production environment first

## Support

For issues or questions, check the system logs:
```bash
journalctl -xe
```
