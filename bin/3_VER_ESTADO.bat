@echo off
REM ===========================================================================
REM  VER_ESTADO.bat - DOBLE CLIC para saber si la carga esta trabajando.
REM  No modifica nada. Solo mira y reporta.
REM ===========================================================================
title Estado de la carga INMOGES
cd /d "%~dp0.."
py -u src\estado.py
echo.
echo    ------------------------------------------------------------------
echo      Cierra esta ventana cuando termines de leer.
echo    ------------------------------------------------------------------
pause
