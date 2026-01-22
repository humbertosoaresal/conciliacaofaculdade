# config.py
import streamlit.column_config as st_col_config
import os

# ==============================================================================
# CONFIGURAÇÃO DE COLUNA (Para visualização)
# ==============================================================================
COL_CONFIG = {
    "Data Lançamento": st_col_config.TextColumn("Data Lançamento"),
    # Adicione suas outras configurações de coluna se houver
}

# ==============================================================================
# CONFIGURAÇÃO E FUNÇÃO DE LOGOS DOS BANCOS
# ==============================================================================
# **ATENÇÃO:** Crie uma pasta chamada 'logos' no mesmo diretório do app.py
# e salve as imagens (ex: bancodobrasil.png) dentro dela.

BANCO_LOGOS = {
    # Mapeamento por CÓDIGO DO BANCO (Você deve completar esta lista)
    '001': 'logos/bancodobrasil.png',
    '748': 'logos/sicredi.png',
    '237': 'logos/bradesco.png',
    # Adicione mais mapeamentos aqui
}

DEFAULT_LOGO_PATH = 'logos/default.png'  # Crie um logo padrão


def get_logo_path(banco_identificador):
    """
    Retorna o caminho do logo com base no identificador do banco.
    """
    nome_normalizado = str(banco_identificador).strip().upper()
    if nome_normalizado in BANCO_LOGOS:
        return BANCO_LOGOS[nome_normalizado]

    for key, path in BANCO_LOGOS.items():
        if key.upper() in nome_normalizado:
            return path

    return DEFAULT_LOGO_PATH


def check_and_display_logo(banco_identificador):
    """Verifica se o logo existe e retorna o caminho para exibição."""
    logo_path = get_logo_path(banco_identificador)
    if os.path.exists(logo_path):
        return logo_path
    else:
        return DEFAULT_LOGO_PATH