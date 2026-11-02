variable "subscription_id" {
  description = "Assinatura Azure onde o ambiente on-prem simulado será criado."
  type        = string
}

variable "location" {
  description = "Região — a mesma do workspace Databricks, para o peering não ter custo de tráfego entre regiões."
  type        = string
  default     = "brazilsouth"
}

variable "resource_group_name" {
  type    = string
  default = "rg-onprem-simulado"
}

variable "vnet_cidr" {
  description = "Faixa do 'datacenter' simulado. Não pode sobrepor a VNet do Databricks (10.139.0.0/16)."
  type        = string
  default     = "10.50.0.0/16"
}

variable "subnet_cidr" {
  type    = string
  default = "10.50.1.0/24"
}

variable "vm_private_ip" {
  description = "IP fixo do servidor on-prem — é o host usado no JDBC e no bootstrap do Kafka."
  type        = string
  default     = "10.50.1.10"
}

variable "vm_size" {
  description = "4 vCPU / 16 GB: suficiente para Db2 + Kafka + Kafka Connect."
  type        = string
  default     = "Standard_D4s_v5"
}

variable "admin_username" {
  type    = string
  default = "azureuser"
}

variable "ssh_public_key_path" {
  type    = string
  default = "~/.ssh/id_ed25519.pub"
}

variable "admin_source_ip" {
  description = "Seu IP público (CIDR /32). Único IP autorizado a acessar a VM via SSH."
  type        = string
}

variable "databricks_workspace_name" {
  type    = string
  default = "adb_databricks_dev1"
}

variable "databricks_resource_group_name" {
  type    = string
  default = "rg_data_engineers"
}

variable "databricks_vnet_cidr" {
  description = "Faixa da VNet gerenciada do Databricks (workers-vnet)."
  type        = string
  default     = "10.139.0.0/16"
}

variable "auto_shutdown_time" {
  description = "Horário (HHmm) de desligamento automático da VM, para não consumir crédito."
  type        = string
  default     = "2300"
}
