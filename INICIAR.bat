@echo off
chcp 65001 >nul
title Sistema de Conciliação Bancária

echo ============================================
echo   Sistema de Conciliação Bancária
echo ============================================
echo.

:: Verifica se o ambiente virtual existe
if not exist "venv\Scripts\activate.bat" (
    echo [ERRO] Sistema não instalado!
    echo Execute primeiro o arquivo INSTALAR.bat
    echo.
    pause
    exit /b 1
)

:: Ativa o ambiente virtual
call venv\Scripts\activate.bat

echo Iniciando o sistema...
echo.
echo O navegador abrirá automaticamente.
echo Para encerrar, feche esta janela ou pressione Ctrl+C
echo.

:: Inicia o Streamlit
streamlit run app.py --server.headless false --browser.gatherUsageStats false

pause
