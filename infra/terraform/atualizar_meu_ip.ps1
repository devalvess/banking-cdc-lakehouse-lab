# Libera o SSH da VM para o seu IP atual. A operadora troca o IP de casa com frequência, e o NSG
# só aceita SSH do IP em admin_source_ip. O script atualiza o terraform.tfvars e roda o apply,
# que mostra o plano e pede confirmação ("yes") antes de mudar qualquer coisa.
# Uso (nesta pasta): .\atualizar_meu_ip.ps1
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

$ip = (Invoke-RestMethod https://api.ipify.org).Trim()
# Compara com o NSG real no Azure (não com o tfvars), para funcionar mesmo depois de um apply cancelado.
$liberado = az network nsg rule show -g rg-onprem-simulado --nsg-name nsg-servidores `
    -n permite-ssh-admin --query sourceAddressPrefix -o tsv

if ($liberado -eq "$ip/32") {
    Write-Host "O NSG já libera o seu IP ($ip). Nada a fazer."
    exit 0
}

Write-Host "IP liberado no NSG: $liberado  ->  seu IP agora: $ip/32"
(Get-Content terraform.tfvars) -replace '^admin_source_ip\s*=.*', "admin_source_ip = `"$ip/32`"" |
    Set-Content terraform.tfvars

# Confira no plano: deve ser "0 to add, 1 to change, 0 to destroy" (só a regra de SSH).
# Digite "yes" à mão: colar o comando junto com uma linha em branco cancela a confirmação.
terraform apply
