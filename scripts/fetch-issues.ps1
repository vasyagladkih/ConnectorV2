<#
.SYNOPSIS
    Быстрая выгрузка и синхронизация открытых задач из GitHub Issues.

.DESCRIPTION
    Скрипт опрашивает GitHub REST API репозитория, извлекает актуальное состояние задач
    (баги, рефакторинг, назначенные исполнители, верификация, перемещения в проекте),
    формирует локальный снимок `issues/ACTIVE_TASKS.md` и выводит сводную таблицу в консоль.

.PARAMETER Number
    Номер конкретного тикета для вывода подробного описания.

.PARAMETER OnlyInWork
    Отображать только задачи, взятые в работу (назначенные или перемещенные в проекте).

.PARAMETER OnlyVerified
    Отображать только задачи и подтвержденные баги.

.PARAMETER Repo
    Репозиторий GitHub в формате 'owner/repo'. По умолчанию: 'vasyagladkih/DataCollector'.

.PARAMETER NoFileWrite
    Не перезаписывать локальный файл `issues/ACTIVE_TASKS.md`, только вывод в stdout.
#>
param (
    [int]$Number = 0,
    [switch]$OnlyInWork,
    [switch]$OnlyVerified,
    [string]$Repo = "vasyagladkih/DataCollector",
    [switch]$NoFileWrite
)

[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$token = [System.Environment]::GetEnvironmentVariable("ISSUES_TOKEN", "User")
if (-not $token) { $token = [System.Environment]::GetEnvironmentVariable("GITHUB_TOKEN", "User") }
if (-not $token) { $token = $env:ISSUES_TOKEN }
if (-not $token) { $token = $env:GITHUB_TOKEN }

if (-not $token) {
    Write-Error "Не найден токен доступа. Установите пользовательскую переменную окружения ISSUES_TOKEN или GITHUB_TOKEN."
    exit 1
}

$headers = @{
    "Authorization" = "Bearer $token"
    "Accept"        = "application/vnd.github+json"
    "X-GitHub-Api-Version" = "2022-11-28"
}

# 1. Загрузка конкретного тикета, если передан -Number
if ($Number -gt 0) {
    $issueUrl = "https://api.github.com/repos/$Repo/issues/$Number"
    try {
        $issue = Invoke-RestMethod -Uri $issueUrl -Headers $headers -Method Get
    } catch {
        Write-Error "Ошибка при получении issue #$Number : $_"
        exit 1
    }

    $type = if ($issue.labels.name -contains "bug") { "BUG" } else { "TASK" }
    $isVerified = if ($issue.body -match '- \[x\]') { $true } else { $false }
    $module = if ($issue.body -match '### Модуль\s+([^\r\n]+)') { $matches[1].Trim() } else { "не указан" }

    Write-Host "================================================================================" -ForegroundColor Cyan
    Write-Host " ISSUE #$($issue.number): $($issue.title)" -ForegroundColor Yellow
    Write-Host " URL: $($issue.html_url)" -ForegroundColor DarkGray
    Write-Host " Тип: $type | Модуль: $module | Статус: $($issue.state)" -ForegroundColor Green
    if ($type -eq "BUG") {
        $verColor = if ($isVerified) { "Green" } else { "DarkYellow" }
        Write-Host " Верифицирован разработчиком: $isVerified" -ForegroundColor $verColor
    }
    if ($issue.assignees.Count -gt 0) {
        Write-Host " Назначен: $(($issue.assignees.login) -join ', ')" -ForegroundColor Cyan
    }
    Write-Host "================================================================================" -ForegroundColor Cyan
    Write-Host ""
    Write-Host $issue.body
    Write-Host ""
    exit 0
}

# 2. Выгрузка всех открытых тикетов
$issuesUrl = "https://api.github.com/repos/$Repo/issues?state=open&per_page=100"
try {
    $issues = Invoke-RestMethod -Uri $issuesUrl -Headers $headers -Method Get
} catch {
    Write-Error "Ошибка при обращении к GitHub API: $_"
    exit 1
}

# 3. Выгрузка событий для выявления ручных перемещений задач в GitHub Projects
$eventsUrl = "https://api.github.com/repos/$Repo/issues/events?per_page=100"
$inWorkSet = [System.Collections.Generic.HashSet[int]]::new()
try {
    $events = Invoke-RestMethod -Uri $eventsUrl -Headers $headers -Method Get
    foreach ($evt in $events) {
        if ($evt.actor.type -eq "User" -and $evt.event -eq "project_v2_item_status_changed") {
            [void]$inWorkSet.Add($evt.issue.number)
        }
    }
} catch {
    # Если события недоступны, продолжаем без критической ошибки
}

# 4. Обработка и нормализация списка задач
$parsedList = [System.Collections.Generic.List[PSCustomObject]]::new()

foreach ($i in $issues) {
    # Исключаем Pull Requests (GitHub возвращает PR в выдаче issues)
    if ($null -ne $i.pull_request) { continue }

    $type = if ($i.labels.name -contains "bug") { "BUG" } else { "TASK" }
    $cleanTitle = $i.title -replace '^\[(BUG|TASK)\]:\s*', ''
    $module = if ($i.body -match '### Модуль\s+([^\r\n]+)') { $matches[1].Trim() } else { "общий" }
    $isVerified = if ($i.body -match '- \[x\]') { $true } else { $false }
    $assignees = ($i.assignees.login) -join ', '
    $isInWork = ($inWorkSet.Contains($i.number)) -or ($assignees.Length -gt 0)

    $parsedList.Add([PSCustomObject]@{
        Number     = $i.number
        Type       = $type
        InWork     = $isInWork
        Verified   = if ($type -eq "BUG") { $isVerified } else { "—" }
        Module     = $module
        Title      = $cleanTitle
        Assignees  = if ($assignees) { $assignees } else { "—" }
        Url        = $i.html_url
        Body       = $i.body
    })
}

# 5. Генерация файла issues/ACTIVE_TASKS.md (полный снимок)
if (-not $NoFileWrite) {
    $projectRoot = Split-Path -Parent $PSScriptRoot
    $issuesDir = Join-Path $projectRoot "issues"
    if (-not (Test-Path $issuesDir)) {
        New-Item -ItemType Directory -Path $issuesDir -Force | Out-Null
    }
    $mdFile = Join-Path $issuesDir "ACTIVE_TASKS.md"

    $sb = [System.Text.StringBuilder]::new()
    [void]$sb.AppendLine("# Реестр активных задач проекта DataCollector")
    [void]$sb.AppendLine("")
    [void]$sb.AppendLine("> Дата обновления: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') | Всего открыто: $($parsedList.Count)")
    [void]$sb.AppendLine("")

    # Задачи в работе
    $inWorkItems = $parsedList | Where-Object { $_.InWork }
    [void]$sb.AppendLine("## В работе (In Progress)")
    if ($inWorkItems.Count -eq 0) {
        [void]$sb.AppendLine("_На данный момент ни одна задача не взята в работу._")
    } else {
        [void]$sb.AppendLine("| # | Тип | Модуль | Название | Исполнитель | Ссылка |")
        [void]$sb.AppendLine("|---|---|---|---|---|---|")
        foreach ($item in $inWorkItems) {
            [void]$sb.AppendLine("| #$($item.Number) | $($item.Type) | $($item.Module) | $($item.Title) | $($item.Assignees) | [GitHub]($($item.Url)) |")
        }
    }
    [void]$sb.AppendLine("")

    # Дефекты (Bugs)
    $bugItems = $parsedList | Where-Object { $_.Type -eq "BUG" }
    [void]$sb.AppendLine("## Дефекты (Bugs)")
    [void]$sb.AppendLine("| # | Модуль | Верифицирован | Название | Ссылка |")
    [void]$sb.AppendLine("|---|---|:---:|---|---|")
    foreach ($item in $bugItems) {
        $verMark = if ($item.Verified -eq $true) { " Да" } else { " Нет" }
        [void]$sb.AppendLine("| #$($item.Number) | $($item.Module) | $verMark | $($item.Title) | [GitHub]($($item.Url)) |")
    }
    [void]$sb.AppendLine("")

    # Задачи рефакторинга (Tasks)
    $taskItems = $parsedList | Where-Object { $_.Type -eq "TASK" }
    [void]$sb.AppendLine("## Задачи на рефакторинг и архитектуру (Tasks)")
    [void]$sb.AppendLine("| # | Модуль | Название | Ссылка |")
    [void]$sb.AppendLine("|---|---|---|---|")
    foreach ($item in $taskItems) {
        [void]$sb.AppendLine("| #$($item.Number) | $($item.Module) | $($item.Title) | [GitHub]($($item.Url)) |")
    }
    [void]$sb.AppendLine("")

    # Детализация каждой задачи
    [void]$sb.AppendLine("## Полный контекст задач")
    foreach ($item in $parsedList) {
        [void]$sb.AppendLine("### [#$($item.Number)] $($item.Title)")
        [void]$sb.AppendLine("- **Тип:** $($item.Type) | **Модуль:** $($item.Module) | **Ссылка:** $($item.Url)")
        if ($item.Type -eq "BUG") {
            [void]$sb.AppendLine("- **Верифицирован:** $($item.Verified)")
        }
        [void]$sb.AppendLine("")
        $null = $sb.AppendLine('```text')
        $null = $sb.AppendLine($item.Body.Trim())
        $null = $sb.AppendLine('```')
        $null = $sb.AppendLine("")
    }

    [System.IO.File]::WriteAllText($mdFile, $sb.ToString(), [System.Text.Encoding]::UTF8)
}

# 6. Вывод таблицы в консоль
$displayList = @($parsedList)
if ($OnlyInWork) { $displayList = @($displayList | Where-Object { $_.InWork }) }
if ($OnlyVerified) { $displayList = @($displayList | Where-Object { ($_.Type -ne "BUG") -or ($_.Verified -eq $true) }) }

Write-Host ""
Write-Host "=== Активные задачи ($($displayList.Count) из $($parsedList.Count)) ===" -ForegroundColor Cyan
$displayList | Select-Object @{N='#';E={"#$($_.Number)"}}, Type, @{N='В работе';E={if($_.InWork){"ДА"}else{"—"}}}, @{N='Проверен';E={$_.Verified}}, Module, Title | Format-Table -AutoSize
Write-Host "Снимок сохранен: issues/ACTIVE_TASKS.md" -ForegroundColor DarkGray
Write-Host "Детали конкретной задачи: .\scripts\fetch-issues.ps1 -Number [номер]" -ForegroundColor DarkGray
