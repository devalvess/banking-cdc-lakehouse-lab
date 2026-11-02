# Ambiente "on-premises" simulado: uma VNet isolada fazendo o papel do datacenter do banco,
# com um servidor Linux rodando Db2, Kafka e Debezium. A ligação com o Databricks é feita por
# VNet peering — no banco real, seria ExpressRoute ou VPN site-to-site.

locals {
  tags = {
    projeto  = "banking-cdc-lakehouse-lab"
    ambiente = "onprem-simulado"
  }
}

resource "azurerm_resource_group" "onprem" {
  name     = var.resource_group_name
  location = var.location
  tags     = local.tags
}

# ---------------------------------------------------------------------------------------------
# Rede
# ---------------------------------------------------------------------------------------------

resource "azurerm_virtual_network" "onprem" {
  name                = "vnet-onprem"
  location            = azurerm_resource_group.onprem.location
  resource_group_name = azurerm_resource_group.onprem.name
  address_space       = [var.vnet_cidr]
  tags                = local.tags
}

resource "azurerm_subnet" "servers" {
  name                 = "snet-servidores"
  resource_group_name  = azurerm_resource_group.onprem.name
  virtual_network_name = azurerm_virtual_network.onprem.name
  address_prefixes     = [var.subnet_cidr]
}

resource "azurerm_network_security_group" "servers" {
  name                = "nsg-servidores"
  location            = azurerm_resource_group.onprem.location
  resource_group_name = azurerm_resource_group.onprem.name
  tags                = local.tags

  # Administração: SSH apenas a partir do IP do administrador.
  security_rule {
    name                       = "permite-ssh-admin"
    priority                   = 100
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefix      = var.admin_source_ip
    destination_address_prefix = "*"
  }

  # Dados: Db2 (50000), Kafka (9092) e SFTP (2222) somente a partir da rede do Databricks.
  security_rule {
    name                       = "permite-databricks-dados"
    priority                   = 200
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_ranges    = ["50000", "9092", "2222"]
    source_address_prefix      = var.databricks_vnet_cidr
    destination_address_prefix = "*"
  }

  # Sobrescreve a regra padrão AllowVnetInBound: nada além do explicitado acima entra.
  security_rule {
    name                       = "nega-demais-entradas"
    priority                   = 4000
    direction                  = "Inbound"
    access                     = "Deny"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }
}

resource "azurerm_subnet_network_security_group_association" "servers" {
  subnet_id                 = azurerm_subnet.servers.id
  network_security_group_id = azurerm_network_security_group.servers.id
}

# ---------------------------------------------------------------------------------------------
# Peering com a VNet gerenciada do Databricks (simula o ExpressRoute)
# ---------------------------------------------------------------------------------------------

data "azurerm_databricks_workspace" "dbx" {
  name                = var.databricks_workspace_name
  resource_group_name = var.databricks_resource_group_name
}

resource "azurerm_databricks_virtual_network_peering" "dbx_to_onprem" {
  name                          = "peer-databricks-to-onprem"
  resource_group_name           = var.databricks_resource_group_name
  workspace_id                  = data.azurerm_databricks_workspace.dbx.id
  remote_address_space_prefixes = azurerm_virtual_network.onprem.address_space
  remote_virtual_network_id     = azurerm_virtual_network.onprem.id
  allow_virtual_network_access  = true

  # O Azure recusa o peering enquanto a VNet está "Updating" (criação da subnet/NSG).
  depends_on = [azurerm_subnet_network_security_group_association.servers]
}

resource "azurerm_virtual_network_peering" "onprem_to_dbx" {
  name                         = "peer-onprem-to-databricks"
  resource_group_name          = azurerm_resource_group.onprem.name
  virtual_network_name         = azurerm_virtual_network.onprem.name
  remote_virtual_network_id    = azurerm_databricks_virtual_network_peering.dbx_to_onprem.virtual_network_id
  allow_virtual_network_access = true
}

# ---------------------------------------------------------------------------------------------
# Servidor on-prem (Db2 + Kafka + Debezium via Docker)
# ---------------------------------------------------------------------------------------------

# IP público usado só para SSH de administração (portas de dados não são expostas).
# Em produção, o acesso administrativo seria via Azure Bastion ou jump server.
resource "azurerm_public_ip" "admin" {
  name                = "pip-vm-onprem-admin"
  location            = azurerm_resource_group.onprem.location
  resource_group_name = azurerm_resource_group.onprem.name
  allocation_method   = "Static"
  sku                 = "Standard"
  tags                = local.tags
}

resource "azurerm_network_interface" "vm" {
  name                = "nic-vm-onprem"
  location            = azurerm_resource_group.onprem.location
  resource_group_name = azurerm_resource_group.onprem.name
  tags                = local.tags

  ip_configuration {
    name                          = "ipconfig"
    subnet_id                     = azurerm_subnet.servers.id
    private_ip_address_allocation = "Static"
    private_ip_address            = var.vm_private_ip
    public_ip_address_id          = azurerm_public_ip.admin.id
  }
}

resource "azurerm_linux_virtual_machine" "onprem" {
  name                  = "vm-onprem-db2"
  location              = azurerm_resource_group.onprem.location
  resource_group_name   = azurerm_resource_group.onprem.name
  size                  = var.vm_size
  admin_username        = var.admin_username
  network_interface_ids = [azurerm_network_interface.vm.id]
  custom_data           = base64encode(file("${path.module}/cloud-init.yaml"))
  tags                  = local.tags

  admin_ssh_key {
    username   = var.admin_username
    public_key = file(pathexpand(var.ssh_public_key_path))
  }

  os_disk {
    caching              = "ReadWrite"
    storage_account_type = "StandardSSD_LRS"
    disk_size_gb         = 64
  }

  source_image_reference {
    publisher = "Canonical"
    offer     = "0001-com-ubuntu-server-jammy"
    sku       = "22_04-lts-gen2"
    version   = "latest"
  }
}

resource "azurerm_dev_test_global_vm_shutdown_schedule" "onprem" {
  virtual_machine_id    = azurerm_linux_virtual_machine.onprem.id
  location              = azurerm_resource_group.onprem.location
  enabled               = true
  daily_recurrence_time = var.auto_shutdown_time
  timezone              = "E. South America Standard Time"

  notification_settings {
    enabled = false
  }
}
