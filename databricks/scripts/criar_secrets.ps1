# Cria o secret scope "onprem" com as credenciais do servidor on-prem simulado.
# As senhas vão da VM direto para o Databricks (pipe), sem aparecer na tela nem em arquivo local.
# Uso: .\databricks\scripts\criar_secrets.ps1 -Profile <PERFIL> -VmIp <IP_DA_VM>
param(
    [Parameter(Mandatory)] [string] $Profile,
    [Parameter(Mandatory)] [string] $VmIp
)
$ErrorActionPreference = "Stop"

$escopos = databricks secrets list-scopes --profile $Profile -o json | ConvertFrom-Json
if (-not ($escopos | Where-Object name -eq "onprem")) {
    databricks secrets create-scope onprem --profile $Profile
}

"svcdbx" | databricks secrets put-secret onprem db2-usuario --profile $Profile
ssh "azureuser@$VmIp" "grep ^SVCDBX_PASSWORD= /opt/onprem/.env | cut -d= -f2- | tr -d '\n'" |
    databricks secrets put-secret onprem db2-senha --profile $Profile
ssh "azureuser@$VmIp" "grep ^SFTP_PASSWORD= /opt/onprem/.env | cut -d= -f2- | tr -d '\n'" |
    databricks secrets put-secret onprem sftp-senha --profile $Profile

databricks secrets list-secrets onprem --profile $Profile
