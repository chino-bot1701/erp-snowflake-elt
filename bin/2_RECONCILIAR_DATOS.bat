@echo off
setlocal
title Reconciliar datos INMOGES - Almena
color 0B
cd /d "%~dp0"

echo.
echo  ================================================================
echo    RECONCILIAR  -  INMOGES manda: si un dato cambio, se corrige
echo  ================================================================
echo.
echo    Le vuelve a preguntar a INMOGES por tramos de historia y compara
echo    huella por huella. Solo toca las filas que INMOGES cambio; las
echo    que estan bien no se tocan. No duplica nada.
echo.
echo    Corre 3 HORAS y para solo. La proxima vez sigue donde se quedo.
echo    Puedes minimizar esta ventana. NO la cierres.
echo.
echo  ----------------------------------------------------------------
echo.

set "LOG=%LOCALAPPDATA%\erp_reconciliar.log"
echo.>> "%LOG%"
echo ======== ARRANQUE %DATE% %TIME% ========>> "%LOG%"

py -u src\reconciliar.py --horas 3 >> "%LOG%" 2>&1
set "RC=%ERRORLEVEL%"

echo.
echo  ----------------------------------------------------------------
if "%RC%"=="0" goto :ok
goto :con_errores

:ok
color 0A
echo.
echo    LISTO. Termino SIN ERRORES.
echo.
goto :resumen

:con_errores
color 0E
echo.
echo    TERMINO, pero hubo errores en alguna ventana.
echo    No se perdio nada: la proxima corrida las reintenta.
echo.
goto :resumen

:resumen
echo  ----------------------------------------------------------------
echo    RESUMEN DE ESTA CORRIDA:
echo  ----------------------------------------------------------------
powershell -NoProfile -Command "Get-Content '%LOG%' -Encoding UTF8 -Tail 22"
echo  ----------------------------------------------------------------
echo.
echo    Log completo:  %LOG%
echo.
pause
endlocal
