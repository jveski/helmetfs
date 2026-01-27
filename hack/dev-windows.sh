#!/usr/bin/env bash
set -euo pipefail

# Azure dev environment for testing Windows WebDAV with helmetfs
#
# Creates a resource group with:
# - Linux VM running helmetfs in Docker
# - Windows Server 2022 VM for RDP access
#
# Usage: ./hack/dev-windows.sh
# Environment variables:
#   LOCATION - Azure region (default: eastus)
#   PREFIX   - Resource name prefix (default: helmetfs-dev)

# Configuration
LOCATION="${LOCATION:-eastus}"
PREFIX="${PREFIX:-helmetfs-dev}"
SUFFIX=$(openssl rand -hex 4)
RESOURCE_GROUP="${PREFIX}-${SUFFIX}"
VNET_NAME="vnet"
SUBNET_NAME="default"
NSG_NAME="nsg"
LINUX_VM_NAME="linux"
WINDOWS_VM_NAME="windows"

# VM sizes
LINUX_VM_SIZE="Standard_B2s"
WINDOWS_VM_SIZE="Standard_B2ms"

# Check prerequisites
command -v az >/dev/null 2>&1 || {
	echo "Error: Azure CLI (az) is required but not installed."
	echo "Install it from: https://docs.microsoft.com/en-us/cli/azure/install-azure-cli"
	exit 1
}

az account show >/dev/null 2>&1 || {
	echo "Error: Not logged into Azure CLI."
	echo "Run: az login"
	exit 1
}

# Generate credentials
ADMIN_USER="azureuser"
# Azure requires: 12+ chars, uppercase, lowercase, number, special char
ADMIN_PASS="$(openssl rand -base64 12)Aa1!"

echo "Creating Azure dev environment..."
echo "  Resource group: $RESOURCE_GROUP"
echo "  Location: $LOCATION"
echo ""

# Create cloud-init for Linux VM
CLOUD_INIT=$(mktemp)
cat >"$CLOUD_INIT" <<'EOF'
#cloud-config
package_update: true
packages:
  - docker.io
runcmd:
  - systemctl enable docker
  - systemctl start docker
  - mkdir -p /var/lib/helmetfs
  - docker pull ghcr.io/jveski/helmetfs:latest
  - docker run -d --name helmetfs --restart=always -p 8080:8080 -v /var/lib/helmetfs:/data -w /data ghcr.io/jveski/helmetfs:latest -addr 0.0.0.0:8080 -debug
EOF

cleanup_temp() {
	rm -f "$CLOUD_INIT"
}
trap cleanup_temp EXIT

# Create resource group
echo "Creating resource group..."
az group create \
	--name "$RESOURCE_GROUP" \
	--location "$LOCATION" \
	--output none

# Create virtual network
echo "Creating virtual network..."
az network vnet create \
	--resource-group "$RESOURCE_GROUP" \
	--name "$VNET_NAME" \
	--address-prefix "10.0.0.0/16" \
	--subnet-name "$SUBNET_NAME" \
	--subnet-prefix "10.0.1.0/24" \
	--output none

# Create network security group
echo "Creating network security group..."
az network nsg create \
	--resource-group "$RESOURCE_GROUP" \
	--name "$NSG_NAME" \
	--output none

# Add NSG rules
echo "Configuring firewall rules..."
az network nsg rule create \
	--resource-group "$RESOURCE_GROUP" \
	--nsg-name "$NSG_NAME" \
	--name "AllowSSH" \
	--priority 1000 \
	--destination-port-ranges 22 \
	--protocol Tcp \
	--access Allow \
	--output none

az network nsg rule create \
	--resource-group "$RESOURCE_GROUP" \
	--nsg-name "$NSG_NAME" \
	--name "AllowRDP" \
	--priority 1001 \
	--destination-port-ranges 3389 \
	--protocol Tcp \
	--access Allow \
	--output none

# Associate NSG with subnet
az network vnet subnet update \
	--resource-group "$RESOURCE_GROUP" \
	--vnet-name "$VNET_NAME" \
	--name "$SUBNET_NAME" \
	--network-security-group "$NSG_NAME" \
	--output none

# Create Linux VM
echo "Creating Linux VM (this may take a few minutes)..."
az vm create \
	--resource-group "$RESOURCE_GROUP" \
	--name "$LINUX_VM_NAME" \
	--image "Ubuntu2404" \
	--size "$LINUX_VM_SIZE" \
	--vnet-name "$VNET_NAME" \
	--subnet "$SUBNET_NAME" \
	--admin-username "$ADMIN_USER" \
	--generate-ssh-keys \
	--public-ip-sku Standard \
	--custom-data "$CLOUD_INIT" \
	--output none

# Create Windows VM
echo "Creating Windows VM (this may take a few minutes)..."
az vm create \
	--resource-group "$RESOURCE_GROUP" \
	--name "$WINDOWS_VM_NAME" \
	--image "Win2022Datacenter" \
	--size "$WINDOWS_VM_SIZE" \
	--vnet-name "$VNET_NAME" \
	--subnet "$SUBNET_NAME" \
	--admin-username "$ADMIN_USER" \
	--admin-password "$ADMIN_PASS" \
	--public-ip-sku Standard \
	--output none

# Get IP addresses
LINUX_PUBLIC_IP=$(az vm show \
	--resource-group "$RESOURCE_GROUP" \
	--name "$LINUX_VM_NAME" \
	--show-details \
	--query publicIps \
	--output tsv)

LINUX_PRIVATE_IP=$(az vm show \
	--resource-group "$RESOURCE_GROUP" \
	--name "$LINUX_VM_NAME" \
	--show-details \
	--query privateIps \
	--output tsv)

WINDOWS_PUBLIC_IP=$(az vm show \
	--resource-group "$RESOURCE_GROUP" \
	--name "$WINDOWS_VM_NAME" \
	--show-details \
	--query publicIps \
	--output tsv)

echo ""
echo "========================================"
echo "Azure dev environment created!"
echo "========================================"
echo ""
echo "Resource group: $RESOURCE_GROUP"
echo ""
echo "Linux VM (helmetfs):"
echo "  Public IP:  $LINUX_PUBLIC_IP"
echo "  Private IP: $LINUX_PRIVATE_IP"
echo "  SSH:        ssh $ADMIN_USER@$LINUX_PUBLIC_IP"
echo ""
echo "Windows VM:"
echo "  Public IP:  $WINDOWS_PUBLIC_IP"
echo "  Username:   $ADMIN_USER"
echo "  Password:   $ADMIN_PASS"
echo "  RDP:        mstsc /v:$WINDOWS_PUBLIC_IP"
echo ""
echo "WebDAV URL (from Windows VM):"
echo "  http://$LINUX_PRIVATE_IP:8080/"
echo ""
echo "To mount on Windows:"
echo "  1. RDP into the Windows VM"
echo "  2. Open File Explorer > This PC > Map network drive"
echo "  3. Enter: http://$LINUX_PRIVATE_IP:8080/"
echo "  Or run: net use Z: http://$LINUX_PRIVATE_IP:8080/"
echo ""
echo "Note: helmetfs may take 1-2 minutes to start after VM creation"
echo "      Check status: ssh $ADMIN_USER@$LINUX_PUBLIC_IP 'docker logs helmetfs'"
echo ""
echo "Cleanup:"
echo "  az group delete -n $RESOURCE_GROUP --yes --no-wait"
echo ""
