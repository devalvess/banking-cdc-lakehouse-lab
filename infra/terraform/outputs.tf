output "vm_private_ip" {
  description = "Host usado pelo Databricks no JDBC (Db2), no bootstrap do Kafka e no SFTP."
  value       = azurerm_network_interface.vm.private_ip_address
}

output "vm_admin_public_ip" {
  value = azurerm_public_ip.admin.ip_address
}

output "ssh_command" {
  value = "ssh ${var.admin_username}@${azurerm_public_ip.admin.ip_address}"
}
