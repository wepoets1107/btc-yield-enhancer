@echo off
chcp 65001 >nul
title BTC 收益增强策略 - 停止

cd /d "%~dp0"

echo ════════════════════════════════════════
echo   BTC 收益增强策略 - 停止
echo ════════════════════════════════════════

:: 查找 Python
set PYTHON=
for %%d in (
    "%HOMEDRIVE%%HOMEPATH%\.workbuddy\binaries\python\envs\default\Scripts\python.exe"
    "%HOMEDRIVE%%HOMEPATH%\.workbuddy\binaries\python\versions\3.13.12\python.exe"
    "%LocalAppData%\Programs\Python\Python313\python.exe"
    "%LocalAppData%\Programs\Python\Python312\python.exe"
    "%LocalAppData%\Programs\Python\Python314\python.exe"
) do if exist %%d set "PYTHON=%%~f" & goto :found
where python >nul 2>&1 && set "PYTHON=python" && goto :found
goto :kill

:found
echo [1/3] 通知策略停止...
"%PYTHON%" -c "import urllib.request; print(urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:5050/api/stop', method='POST'), timeout=5).read().decode())" 2>nul || echo 服务未运行

:kill
echo [2/3] 清理端口进程...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":5050 "') do (
    taskkill /F /PID %%a >nul 2>&1 && echo 已终止 PID %%a
)
timeout /t 1 /nobreak >nul

echo [3/3] 二次确认...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":5050 "') do (
    taskkill /F /PID %%a >nul 2>&1
)

echo.
echo ✅ 策略已停止
echo ════════════════════════════════════════
