@echo off
chcp 65001 >nul
title BTC 收益增强策略

cd /d "%~dp0"

echo ════════════════════════════════════════
echo   BTC 收益增强策略 - 启动
echo ════════════════════════════════════════

:: 1. 加载 .env 文件中的环境变量
echo [1/5] 加载环境变量...
if exist "%~dp0.env" (
    for /f "usebackq tokens=1,2 delims==" %%a in ("%~dp0.env") do (
        set "%%a=%%b"
    )
)

:: 2. 查找可用的 Python
echo [2/5] 检测 Python...
set PYTHON=
:: 优先用 workbuddy 托管的 Python
for %%d in (
    "%HOMEDRIVE%%HOMEPATH%\.workbuddy\binaries\python\envs\default\Scripts\python.exe"
    "%HOMEDRIVE%%HOMEPATH%\.workbuddy\binaries\python\versions\3.13.12\python.exe"
    "%LocalAppData%\Programs\Python\Python313\python.exe"
    "%LocalAppData%\Programs\Python\Python312\python.exe"
    "%LocalAppData%\Programs\Python\Python314\python.exe"
) do (
    if exist %%d (
        set "PYTHON=%%d"
        goto :found_python
    )
)
:: 兜底：用 PATH 里的 python
where python >nul 2>&1
if %errorlevel%==0 (set "PYTHON=python" && goto :found_python)

echo ❌ 未找到 Python，请安装或设置路径
pause
exit /b 1

:found_python
echo   使用 Python: %PYTHON%

:: 3. 清理旧进程
echo [3/5] 清理旧进程...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr ":5050 "') do (
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 2 /nobreak >nul

:: 4. 启动 Flask 服务
echo [4/5] 启动 Flask 服务...
start "BTC 收益增强 · 后端" "%PYTHON%" app.py
timeout /t 5 /nobreak >nul

:: 5. 启动策略
echo [5/5] 启动策略...
"%PYTHON%" -c "import urllib.request; print(urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:5050/api/start', method='POST'), timeout=60).read().decode())"

:: 打开仪表盘
start http://127.0.0.1:5050/

echo.
echo ✅ 启动完成！仪表盘已打开。
echo    后端服务在独立窗口运行。
echo    如需停止策略，运行 stop.bat
echo ════════════════════════════════════════
