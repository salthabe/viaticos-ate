@echo off
echo ================================================
echo   SISTEMA DE VIATICOS ATE
echo ================================================
echo.

:: Verificar Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python no encontrado. Instala Python desde https://python.org
    pause
    exit /b 1
)

:: Instalar dependencias si no existen
echo Verificando dependencias...
pip show fastapi >nul 2>&1
if errorlevel 1 (
    echo Instalando dependencias por primera vez...
    pip install -r requirements.txt
)

:: Obtener IP local para mostrarla
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /i "IPv4" ^| findstr /v "169.254"') do (
    set LOCAL_IP=%%a
    goto :found
)
:found
set LOCAL_IP=%LOCAL_IP: =%

echo.
echo ================================================
echo   Acceso en ESTA PC:     http://localhost:8000
echo   Acceso desde la RED:   http://%LOCAL_IP%:8000
echo   (compartir esa URL con otras notebooks)
echo ================================================
echo.
echo Presiona Ctrl+C para detener el servidor.
echo.

start http://localhost:8000
python -m uvicorn app:app --host 0.0.0.0 --port 8000

pause
