# Cria o service principal de FinOps (só leitura de custos) e guarda as credenciais no secret
# scope "finops". A senha vai do Azure direto para o Databricks, sem aparecer na tela.
# Uso: .\databricks\scripts\criar_sp_finops.ps1 -Profile <PERFIL>
param(
    [Parameter(Mandatory)] [string] $Profile,
    [string] $SubscriptionId = (az account show --query id -o tsv)
)
$ErrorActionPreference = "Stop"

# Cost Management Reader: lê custos e orçamentos, não enxerga nem altera recursos.
$sp = az ad sp create-for-rbac --name "sp-banking-lab-finops" `
    --role "Cost Management Reader" --scopes "/subscriptions/$SubscriptionId" -o json | ConvertFrom-Json

$escopos = databricks secrets list-scopes --profile $Profile -o json | ConvertFrom-Json
if (-not ($escopos | Where-Object name -eq "finops")) {
    databricks secrets create-scope finops --profile $Profile
}

$sp.tenant       | databricks secrets put-secret finops tenant-id --profile $Profile
$sp.appId        | databricks secrets put-secret finops client-id --profile $Profile
$sp.password     | databricks secrets put-secret finops client-secret --profile $Profile
$SubscriptionId  | databricks secrets put-secret finops subscription-id --profile $Profile

databricks secrets list-secrets finops --profile $Profile
