<#
.SYNOPSIS
    Создание тикетов в GitHub Issues для проекта DataCollector.
.EXAMPLE
    .\scripts\create-issue.ps1 -Type bug -Module data-ingestor -Title "Утечка Netty памяти" -Body "Описание дефекта..."
    .\scripts\create-issue.ps1 -Type task -Module common -Title "Доработка схемы" -Body "Описание задачи..."
#>
param (
    [Parameter(Mandatory = $true)]
    [ValidateSet('bug', 'task')]
    [string]$Type,

    [Parameter(Mandatory = $false)]
    [ValidateSet('data-ingestor', 'control-plane', 'common', 'infrastructure')]
    [string]$Module = 'data-ingestor',

    [Parameter(Mandatory = $true)]
    [string]$Title,

    [Parameter(Mandatory = $true)]
    [string]$Body,

    [Parameter(Mandatory = $false)]
    [string]$Repo = 'vasyagladkih/DataCollector',

    [Parameter(Mandatory = $false)]
    [string]$TokenVar = 'ISSUES_TOKEN'
)

$ErrorActionPreference = 'Stop'

# Получение токена из окружения пользователя или текущего сеанса
$token = [System.Environment]::GetEnvironmentVariable($TokenVar, 'User')
if (-not $token) {
    $token = [System.Environment]::GetEnvironmentVariable($TokenVar, 'Process')
}
if (-not $token) {
    $token = [System.Environment]::GetEnvironmentVariable('GITHUB_TOKEN', 'User')
}
if (-not $token) {
    $token = [System.Environment]::GetEnvironmentVariable('GITHUB_TOKEN', 'Process')
}

if (-not $token) {
    Write-Error "Токен не найден! Задайте переменную окружения '$TokenVar' или 'GITHUB_TOKEN'."
    exit 1
}

# Формирование префикса заголовка и тела тикета
if ($Type -eq 'bug') {
    $prefix = "[BUG]: "
    $fullTitle = if ($Title.StartsWith($prefix)) { $Title } else { "$prefix$Title" }
    $labels = @('bug')
    $issueBody = @"
### Верификация
- [ ] Баг проверен мной лично (не непроверенная гипотеза ИИ)

### Модуль
$Module

### Что сломано и где
$Body
"@
} else {
    $prefix = "[TASK]: "
    $fullTitle = if ($Title.StartsWith($prefix)) { $Title } else { "$prefix$Title" }
    $labels = @('task')
    $issueBody = @"
### Модуль
$Module

### Что нужно сделать
$Body
"@
}

$payloadObj = @{
    title  = $fullTitle
    body   = $issueBody
    labels = $labels
}

$jsonPayload = $payloadObj | ConvertTo-Json -Depth 5 -Compress
$bytes = [System.Text.Encoding]::UTF8.GetBytes($jsonPayload)

$headers = @{
    'Authorization'          = "Bearer $token"
    'Accept'                 = 'application/vnd.github+json'
    'X-GitHub-Api-Version'   = '2022-11-28'
    'User-Agent'             = 'DataCollector-Issue-Script'
}

try {
    $response = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/issues" `
        -Method Post `
        -Headers $headers `
        -Body $bytes `
        -ContentType 'application/json; charset=utf-8'

    Write-Output "SUCCESS: Issue #$($response.number) создан успешно!"
    Write-Output "URL: $($response.html_url)"
    Write-Output "Title: $($response.title)"
} catch {
    Write-Error "Ошибка при обращении к GitHub API: $($_.Exception.Message)"
    if ($_.ErrorDetails.Message) {
        Write-Error "Детали: $($_.ErrorDetails.Message)"
    }
    exit 1
}
