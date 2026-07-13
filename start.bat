@echo off
chcp 65001 >nul
title BTC 收益增强策略

cd /d "%~dp0"

echo ==============================
echo   BTC 收益增强策略 - 启动
echo ==============================

:: 1. 加载 .env
echo [1/3] 加载环境变量...
if not exist ".env" (
    echo [错误] .env 文件不存在！
    echo 请复制 .env.example 重命名为 .env 并填入 API 凭证。
    pause
    exit /b 1
)
for /f "usebackq tokens=1,2 delims==" %%a in (".env") do set "%%a=%%b"
if "%DERIBIT_ID%"=="" (
    echo [错误] DERIBIT_ID 未设置，请检查 .env 文件
    pause
    exit /b 1
)
echo   OK

:: 2. 找 Python
echo [2/3] 检测 Python...
set PYTHON=
for %%d in (
    "%~dp0venv\Scripts\python.exe"
    "%~dp0.venv\Scripts\python.exe"
    "%HOMEDRIVE%%HOMEPATH%\.workbuddy\binaries\python\envs\default\Scripts\python.exe"
    "%HOMEDRIVE%%HOMEPATH%\.workbuddy\binaries\python\versions\3.13.12\python.exe"
    "%LocalAppData%\Programs\Python\Python314\python.exe"
    "%LocalAppData%\Programs\Python\Python313\python.exe"
) do if exist %%d set "PYTHON=%%~f" & goto :found_python
where python >nul 2>&1 && set "PYTHON=python" & goto :found_python
echo [错误] 未找到 Python
pause
exit /b 1

:found_python
echo   使用: %PYTHON%
echo.

:: 3. 启动（后台运行，不依赖新窗口编码）
echo [3/3] 启动服务...
echo   关闭旧进程(如有)...
for /f "tokens=2 delims= " %%a in ('netstat -ano ^| findstr ":5050 " ^| findstr "LISTENING"') do (
    taskkill /F /PID %%a >nul 2>&1
)
timeout /t 2 /nobreak >nul

echo   启动 Flask...
start /B "" "%PYTHON%" app.py
timeout /t 3 /nobreak >nul

:: 等待就绪并初始化引擎
echo   初始化引擎...
for /l %%i in (1,1,20) do (
    >nul 2>&1 curl -s http://127.0.0.1:5050/api/init && (
        echo ? 服务就绪
        goto :done
    )
    >nul ping -n 2 127.0.0.1
)
echo ? 启动超时，请手动打开 http://127.0.0.1:5050/

:done
start http://127.0.0.1:5050/
echo.
echo ? 启动完成！
echo    仪表盘已打开
echo    停止请运行 stop.bat
echo ==============================
