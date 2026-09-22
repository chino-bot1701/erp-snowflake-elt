@echo off
setlocal
title Actualizar datos INMOGES - Almena
color 0B
cd /d "%~dp0"

echo.
echo  ================================================================
echo    ACTUALIZAR DATOS  -  INMOGES a Snowflake
echo  ================================================================
echo.
echo    Trae SOLO lo que cambio en INMOGES desde la ultima vez.
echo    No duplica nada: si un registro ya existe, lo actualiza.
echo.
echo    Suele tardar unos minutos. NO cierres esta ventana.
echo.
echo  ----------------------------------------------------------------
echo.

set "LOG=%LOCALAPPDATA%\erp_sync.log"
py -u src\sync.py >> "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"

echo.
echo  ----------------------------------------------------------------
if "%RC%"=="0" goto :ok
goto :con_errores

:ok
color 0A
echo.
echo    LISTO. La actualizacion termino SIN ERRORES.
echo.
goto :resumen

:con_errores
color 0E
echo.
echo    TERMINO, pero hubo errores en alguna empresa.
echo    No se perdio nada: la proxima corrida los reintenta.
echo.
goto :resumen

:resumen
echo  ----------------------------------------------------------------
echo    RESUMEN DE ESTA CORRIDA:
echo  ----------------------------------------------------------------
powershell -NoProfile -Command "Get-Content '%LOG%' -Encoding UTF8 -Tail 12"
echo  ----------------------------------------------------------------
echo.
echo    Log completo:  %LOG%
echo.
pause
endlocal
