@echo off
chcp 65001 >nul
title Instalador - Sistema de Conciliação Bancária

echo ============================================
echo   INSTALADOR - Sistema de Conciliação Bancária
echo ============================================
echo.

:: Verifica se Python está instalado
python --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERRO] Python não encontrado!
    echo.
    echo Por favor, instale o Python 3.10 ou superior:
    echo https://www.python.org/downloads/
    echo.
    echo IMPORTANTE: Marque a opção "Add Python to PATH" durante a instalação!
    echo.
    pause
    exit /b 1
)

echo [OK] Python encontrado!
echo.

:: Cria ambiente virtual se não existir
if not exist "venv" (
    echo Criando ambiente virtual...
    python -m venv venv
    echo [OK] Ambiente virtual criado!
) else (
    echo [OK] Ambiente virtual já existe!
)
echo.

:: Ativa o ambiente virtual e instala dependências
echo Instalando dependências (pode demorar alguns minutos)...
echo.
call venv\Scripts\activate.bat
pip install --upgrade pip >nul 2>&1
pip install -r requirements.txt

if %errorlevel% neq 0 (
    echo.
    echo [ERRO] Falha ao instalar dependências!
    pause
    exit /b 1
)

echo.
echo ============================================
echo   INSTALAÇÃO CONCLUÍDA COM SUCESSO!
echo ============================================
echo.
echo Para iniciar o sistema, execute: INICIAR.bat
echo.
pause
