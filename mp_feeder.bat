@echo off
TITLE Automacao Comparador de Preco

REM Muda o diretório para a pasta onde o .bat está localizado
cd /d "%~dp0"

echo.
echo Ativando o ambiente virtual...
echo.

REM Ativa o ambiente virtual
call venv\Scripts\activate

echo Ambiente ativado. Iniciando a aplicacao...
echo.

REM Executa o script principal do Python
python main.py

echo.
echo A aplicacao foi fechada.