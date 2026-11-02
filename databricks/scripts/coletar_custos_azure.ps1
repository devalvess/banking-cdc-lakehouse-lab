# Coleta o custo real do projeto na Cost Management API do Azure, com a identidade do az login,
# e envia a resposta para o volume finops.custos_azure. O job finops_custos carrega na tabela.
# Alternativa ao service principal (criar_sp_finops.ps1) para tenants onde não é possível criar um.
# Uso: .\databricks\scripts\coletar_custos_azure.ps1 -Profile <PERFIL> [-Dias 35] [-RodarJob]
param(
    [Parameter(Mandatory)] [string] $Profile,
    [int] $Dias = 35,
    [string] $Catalogo = "banking_lab",
    [string[]] $ResourceGroups = @("rg-onprem-simulado", "databricks-rg-adb_databricks_dev1-otdiskxfuqjga"),
    [string] $TagProjeto = "banking-cdc-lakehouse-lab",
    [switch] $RodarJob
)
$ErrorActionPreference = "Stop"

$assinatura = az account show --query id -o tsv
$fim = (Get-Date).ToUniversalTime().Date
$inicio = $fim.AddDays(-$Dias)

# Mesma consulta do notebook (modo api): custo diário por recurso, medidor e tag carga.
$corpo = @{
    type       = "ActualCost"
    timeframe  = "Custom"
    timePeriod = @{ from = $inicio.ToString("yyyy-MM-dd") + "T00:00:00Z"; to = $fim.ToString("yyyy-MM-dd") + "T23:59:59Z" }
    dataset    = @{
        granularity = "Daily"
        aggregation = @{ totalCost = @{ name = "Cost"; function = "Sum" } }
        grouping    = @(
            @{ type = "Dimension"; name = "ResourceId" },
            @{ type = "Dimension"; name = "MeterCategory" },
            @{ type = "TagKey"; name = "carga" }
        )
        filter      = @{ or = @(
            @{ dimensions = @{ name = "ResourceGroupName"; operator = "In"; values = $ResourceGroups } },
            @{ tags = @{ name = "projeto"; operator = "In"; values = @($TagProjeto) } }
        ) }
    }
}
$arquivoCorpo = New-TemporaryFile
[IO.File]::WriteAllText($arquivoCorpo, ($corpo | ConvertTo-Json -Depth 10))

$url = "https://management.azure.com/subscriptions/$assinatura/providers/Microsoft.CostManagement/query?api-version=2023-11-01"
$colunas = $null
$linhas = @()
while ($url) {
    $resposta = $null
    foreach ($tentativa in 1..5) {
        $saida = az rest --method post --url $url --body "@$arquivoCorpo" -o json 2>$null
        if ($LASTEXITCODE -eq 0) { $resposta = $saida | Out-String | ConvertFrom-Json; break }
        Write-Host "API ocupada (cota por minuto). Nova tentativa em 30 s..."
        Start-Sleep -Seconds 30
    }
    if (-not $resposta) { throw "A Cost Management API não respondeu após 5 tentativas." }
    $colunas = $resposta.properties.columns.name
    $linhas += $resposta.properties.rows
    $url = $resposta.properties.nextLink
}
Remove-Item $arquivoCorpo

$nome = "custos_azure_" + (Get-Date).ToUniversalTime().ToString("yyyyMMdd_HHmmss") + ".json"
$arquivo = Join-Path ([IO.Path]::GetTempPath()) $nome
$coleta = @{ inicio = $inicio.ToString("yyyy-MM-dd"); fim = $fim.ToString("yyyy-MM-dd"); colunas = $colunas; linhas = $linhas }
[IO.File]::WriteAllText($arquivo, ($coleta | ConvertTo-Json -Depth 10 -Compress))
Write-Host "$($linhas.Count) linhas de custo entre $($coleta.inicio) e $($coleta.fim)"

databricks fs cp $arquivo "dbfs:/Volumes/$Catalogo/finops/custos_azure/$nome" --profile $Profile
Remove-Item $arquivo

if ($RodarJob) {
    Push-Location (Join-Path $PSScriptRoot "..")
    databricks bundle run finops -t dev --profile $Profile
    Pop-Location
}
