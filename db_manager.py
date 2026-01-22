import sqlite3
import pandas as pd
from datetime import datetime
import os
import numpy as np
import streamlit as st
from contextlib import contextmanager

# Importa módulo de conexão com banco (SQLite local ou PostgreSQL produção)
from database import (
    get_db_connection, IS_PRODUCTION, get_placeholder,
    execute_query, adapt_schema_for_postgres, get_connection
)

# O nome do arquivo do banco de dados SQLite (usado apenas localmente)
DB_FILE = 'conciliacao_db.sqlite'

# Placeholder para queries parametrizadas (? para SQLite, %s para PostgreSQL)
PH = get_placeholder()

# Tipo de auto-incremento (AUTOINCREMENT para SQLite, SERIAL para PostgreSQL)
if IS_PRODUCTION:
    AUTO_INCREMENT = "SERIAL PRIMARY KEY"
else:
    AUTO_INCREMENT = "INTEGER PRIMARY KEY AUTOINCREMENT"

# Nomes das tabelas
CADASTRO_CONTAS_TABLE = 'cadastro_contas'
EXTRATO_BANCARIO_TABLE = 'extrato_bancario_historico'
MAPEAMENTO_BANCOS_TABLE = 'mapeamento_bancos'
PLANO_CONTAS_TABLE = 'plano_contas'
LANCAMENTOS_CONTABEIS_TABLE = 'lancamentos_contabeis'
EMPRESA_TABLE = 'empresa'
SOCIOS_TABLE = 'socios'
LOGOTIPOS_TABLE = 'logotipos'
PARCELAMENTOS_TABLE = 'parcelamentos'
PARCELAMENTO_DEBITOS_TABLE = 'parcelamento_debitos'
PARCELAMENTO_PARCELAS_TABLE = 'parcelamento_parcelas'
PARCELAMENTO_PAGAMENTOS_TABLE = 'parcelamento_pagamentos'

# Mapeamento centralizado de colunas
CADASTRO_COLS_DB_TO_DF = {
    'Conta_Contabil': 'Conta Contábil',
    'Saldo_Inicial': 'Saldo Inicial',
    'Data_Inicial_Saldo': 'Data Inicial Saldo',
    'Conta_Contabil_Negativo': 'Conta Contábil (-)'
}
CADASTRO_COLS_DF_TO_DB = {v: k for k, v in CADASTRO_COLS_DB_TO_DF.items()}


# Nota: get_db_connection agora é importado do módulo database.py
# Suporta automaticamente SQLite (local) e PostgreSQL (produção)


# ==============================================================================
# FUNÇÕES DE INICIALIZAÇÃO E ESQUEMA
# ==============================================================================

def init_db():
    """Inicializa e cria as tabelas se não existirem, adicionando as novas colunas."""
    with get_db_connection() as conn:
        c = conn.cursor()
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {CADASTRO_CONTAS_TABLE} (
                Agencia TEXT,
                Conta TEXT,
                Conta_OFX_Normalizada TEXT,
                Conta_Contabil TEXT,
                Conta_Contabil_Negativo TEXT,
                Saldo_Inicial REAL DEFAULT 0.0,
                Data_Inicial_Saldo TEXT,
                Codigo_Banco TEXT,
                Path_Logo TEXT,
                UNIQUE(Codigo_Banco, Conta_OFX_Normalizada)
            )
        ''')
        # Adicionar colunas se elas não existirem (para bancos de dados antigos)
        # Usa Exception genérica para funcionar com SQLite e PostgreSQL
        try:
            c.execute(f"ALTER TABLE {CADASTRO_CONTAS_TABLE} ADD COLUMN Codigo_Banco TEXT")
            conn.commit()
        except Exception:
            conn.rollback()  # Coluna já existe
        try:
            c.execute(f"ALTER TABLE {CADASTRO_CONTAS_TABLE} ADD COLUMN Path_Logo TEXT")
            conn.commit()
        except Exception:
            conn.rollback()  # Coluna já existe
        try:
            c.execute(f"ALTER TABLE {CADASTRO_CONTAS_TABLE} ADD COLUMN Conta_Contabil_Negativo TEXT")
            conn.commit()
        except Exception:
            conn.rollback()  # Coluna já existe

        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {EXTRATO_BANCARIO_TABLE} (
                ID_Transacao TEXT PRIMARY KEY,
                Data_Lancamento DATE,
                Valor REAL,
                Descricao TEXT,
                Tipo TEXT,
                Banco_OFX TEXT,
                Conta_OFX_Normalizada TEXT
            )
        ''')
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {MAPEAMENTO_BANCOS_TABLE} (
                Codigo_Banco TEXT PRIMARY KEY,
                Nome_Banco TEXT
            )
        ''')
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {PLANO_CONTAS_TABLE} (
                codigo TEXT PRIMARY KEY,
                classificacao TEXT,
                descricao TEXT,
                tipo TEXT,
                natureza TEXT,
                grau TEXT,
                data_cadastro TEXT,
                encerrada BOOLEAN DEFAULT FALSE,
                data_encerramento TEXT
            )
        ''')
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {LANCAMENTOS_CONTABEIS_TABLE} (
                id {AUTO_INCREMENT},
                idlancamento TEXT,
                data_lancamento DATE,
                historico TEXT,
                valor REAL,
                tipo_lancamento TEXT,
                reduz_deb TEXT,
                nome_conta_d TEXT,
                reduz_cred TEXT,
                nome_conta_c TEXT,
                origem TEXT
            )
        ''')
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {EMPRESA_TABLE} (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                cnpj TEXT UNIQUE,
                razao_social TEXT,
                nome_fantasia TEXT,
                inscricao_estadual TEXT,
                inscricao_municipal TEXT,
                logradouro TEXT,
                numero TEXT,
                complemento TEXT,
                bairro TEXT,
                municipio TEXT,
                uf TEXT,
                cep TEXT,
                telefone TEXT,
                email TEXT,
                data_abertura TEXT,
                situacao TEXT,
                atividade_principal TEXT,
                atividades_secundarias TEXT,
                data_cadastro TEXT
            )
        ''')

        # Adicionar colunas se não existirem (para bancos antigos)
        try:
            c.execute(f"ALTER TABLE {EMPRESA_TABLE} ADD COLUMN inscricao_estadual TEXT")
            conn.commit()
        except Exception:
            conn.rollback()
        try:
            c.execute(f"ALTER TABLE {EMPRESA_TABLE} ADD COLUMN inscricao_municipal TEXT")
            conn.commit()
        except Exception:
            conn.rollback()
        try:
            c.execute(f"ALTER TABLE {EMPRESA_TABLE} ADD COLUMN atividade_principal TEXT")
            conn.commit()
        except Exception:
            conn.rollback()
        try:
            c.execute(f"ALTER TABLE {EMPRESA_TABLE} ADD COLUMN atividades_secundarias TEXT")
            conn.commit()
        except Exception:
            conn.rollback()

        # Criar tabela de sócios
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {SOCIOS_TABLE} (
                id {AUTO_INCREMENT},
                empresa_id INTEGER DEFAULT 1,
                cpf TEXT UNIQUE NOT NULL,
                nome_completo TEXT NOT NULL,
                data_nascimento TEXT,
                logradouro TEXT,
                numero TEXT,
                complemento TEXT,
                bairro TEXT,
                municipio TEXT,
                uf TEXT,
                cep TEXT,
                telefone TEXT,
                email TEXT,
                socio_administrador BOOLEAN DEFAULT FALSE,
                data_cadastro TEXT,
                FOREIGN KEY (empresa_id) REFERENCES {EMPRESA_TABLE}(id)
            )
        ''')

        # Criar tabela de logotipos
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {LOGOTIPOS_TABLE} (
                id {AUTO_INCREMENT},
                empresa_id INTEGER DEFAULT 1,
                nome_arquivo TEXT NOT NULL,
                descricao TEXT,
                caminho_arquivo TEXT NOT NULL,
                logo_principal BOOLEAN DEFAULT FALSE,
                data_upload TEXT,
                FOREIGN KEY (empresa_id) REFERENCES {EMPRESA_TABLE}(id)
            )
        ''')

        # ==========================================
        # TABELAS DE PARCELAMENTOS
        # ==========================================

        # Tabela principal de parcelamentos
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {PARCELAMENTOS_TABLE} (
                id {AUTO_INCREMENT},
                numero_parcelamento TEXT UNIQUE,
                cnpj TEXT,
                orgao TEXT,
                modalidade TEXT,
                situacao TEXT,
                data_adesao TEXT,
                data_consolidacao TEXT,
                data_inicio TEXT,
                data_encerramento TEXT,
                motivo_encerramento TEXT,
                qtd_parcelas INTEGER,
                qtd_pagas INTEGER DEFAULT 0,
                qtd_vencidas INTEGER DEFAULT 0,
                qtd_a_vencer INTEGER DEFAULT 0,
                valor_parcela REAL,
                valor_total_consolidado REAL,
                valor_principal REAL,
                valor_multa REAL,
                valor_juros REAL,
                saldo_devedor REAL,
                conta_contabil_principal TEXT,
                conta_contabil_multa TEXT,
                conta_contabil_juros TEXT,
                conta_contabil_banco TEXT,
                data_cadastro TEXT,
                observacoes TEXT
            )
        ''')

        # Adicionar colunas novas para parcelamentos existentes
        try:
            c.execute(f"ALTER TABLE {PARCELAMENTOS_TABLE} ADD COLUMN data_inicio TEXT")
            conn.commit()
        except Exception:
            conn.rollback()
        try:
            c.execute(f"ALTER TABLE {PARCELAMENTOS_TABLE} ADD COLUMN data_encerramento TEXT")
            conn.commit()
        except Exception:
            conn.rollback()
        try:
            c.execute(f"ALTER TABLE {PARCELAMENTOS_TABLE} ADD COLUMN motivo_encerramento TEXT")
            conn.commit()
        except Exception:
            conn.rollback()

        # Tabela de débitos do parcelamento
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {PARCELAMENTO_DEBITOS_TABLE} (
                id {AUTO_INCREMENT},
                parcelamento_id INTEGER,
                codigo_receita TEXT,
                descricao_receita TEXT,
                periodo_apuracao TEXT,
                data_vencimento TEXT,
                saldo_originario REAL,
                valor_principal REAL,
                valor_multa REAL,
                valor_juros REAL,
                valor_total REAL,
                FOREIGN KEY (parcelamento_id) REFERENCES {PARCELAMENTOS_TABLE}(id)
            )
        ''')

        # Adiciona coluna saldo_originario se não existir
        try:
            c.execute(f"ALTER TABLE {PARCELAMENTO_DEBITOS_TABLE} ADD COLUMN saldo_originario REAL")
            conn.commit()
        except Exception:
            conn.rollback()

        # Tabela de parcelas
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {PARCELAMENTO_PARCELAS_TABLE} (
                id {AUTO_INCREMENT},
                parcelamento_id INTEGER,
                numero_parcela INTEGER,
                data_vencimento TEXT,
                valor_originario REAL,
                valor_principal REAL,
                valor_multa REAL,
                valor_juros REAL,
                valor_encargos REAL,
                saldo_atualizado REAL,
                situacao TEXT,
                data_pagamento TEXT,
                valor_pago REAL,
                id_transacao_banco TEXT,
                FOREIGN KEY (parcelamento_id) REFERENCES {PARCELAMENTOS_TABLE}(id)
            )
        ''')

        # Adiciona colunas novas para PGFN (bancos antigos)
        for col in ['valor_principal', 'valor_multa', 'valor_juros', 'valor_encargos']:
            try:
                c.execute(f"ALTER TABLE {PARCELAMENTO_PARCELAS_TABLE} ADD COLUMN {col} REAL")
                conn.commit()
            except Exception:
                conn.rollback()  # Coluna já existe

        # Tabela de pagamentos (histórico)
        c.execute(f'''
            CREATE TABLE IF NOT EXISTS {PARCELAMENTO_PAGAMENTOS_TABLE} (
                id {AUTO_INCREMENT},
                parcelamento_id INTEGER,
                data_pagamento TEXT,
                valor_pago REAL,
                valor_principal REAL,
                valor_multa REAL,
                valor_juros REAL,
                darf_numero TEXT,
                id_transacao_banco TEXT,
                conciliado BOOLEAN DEFAULT FALSE,
                data_conciliacao TEXT,
                FOREIGN KEY (parcelamento_id) REFERENCES {PARCELAMENTOS_TABLE}(id)
            )
        ''')

        # Adicionar colunas para lançamentos contábeis se não existirem
        for col in ['reduz_deb', 'nome_conta_d', 'reduz_cred', 'nome_conta_c', 'origem', 'idlancamento', 'tipo_lancamento']:
            try:
                c.execute(f"ALTER TABLE {LANCAMENTOS_CONTABEIS_TABLE} ADD COLUMN {col} TEXT")
                conn.commit()
            except Exception:
                conn.rollback()  # Coluna já existe

        # Adicionar colunas para o plano de contas
        for col in ['natureza', 'grau']:
            try:
                c.execute(f"ALTER TABLE {PLANO_CONTAS_TABLE} ADD COLUMN {col} TEXT")
                conn.commit()
            except Exception:
                conn.rollback()  # Coluna já existe

        # Renomear tipo_conta para tipo para compatibilidade
        try:
            c.execute(f"ALTER TABLE {PLANO_CONTAS_TABLE} RENAME COLUMN tipo_conta TO tipo")
            conn.commit()
        except Exception:
            conn.rollback()  # Coluna já existe ou a antiga não existe mais

        conn.commit()


# ==============================================================================
# FUNÇÕES DE CADASTRO DE CONTAS BANCÁRIAS
# ==============================================================================

# CACHE DESABILITADO TEMPORARIAMENTE PARA DEBUGGING
# @st.cache_data(show_spinner="Carregando cadastro do banco de dados...")
def carregar_cadastro_contas() -> pd.DataFrame:
    """Carrega o cadastro de contas do BD, incluindo as novas colunas de banco e logo."""
    try:
        with get_db_connection() as conn:
            query = f"SELECT * FROM {CADASTRO_CONTAS_TABLE} GROUP BY Codigo_Banco, Conta_OFX_Normalizada ORDER BY Agencia, Conta"
            df = pd.read_sql_query(query, conn, dtype={'Data_Inicial_Saldo': str, 'Codigo_Banco': str})
            df.rename(columns=CADASTRO_COLS_DB_TO_DF, inplace=True)
    except Exception:
        init_db()
        st.warning("DB atualizado ou criado. Recarregue a aplicação.")
        return pd.DataFrame()

    if 'Data Inicial Saldo' in df.columns:
        df['Data Inicial Saldo'] = df['Data Inicial Saldo'].replace(['None', 'nan', 'NaT', ''], None).astype(object)

    return df

def salvar_cadastro_contas(df: pd.DataFrame):
    """Salva o DataFrame de cadastro no BD, usando uma estratégia de DELETE + APPEND."""
    db_cols = ['Agencia', 'Conta', 'Conta_OFX_Normalizada', 'Conta_Contabil', 'Conta_Contabil_Negativo', 'Saldo_Inicial', 'Data_Inicial_Saldo', 'Codigo_Banco', 'Path_Logo']
    
    with get_db_connection() as conn:
        c = conn.cursor()
        # Limpa a tabela antes de inserir novos dados
        c.execute(f"DELETE FROM {CADASTRO_CONTAS_TABLE}")

        if df.empty:
            st.warning(f"Tabela '{CADASTRO_CONTAS_TABLE}' foi limpa, pois o DataFrame fornecido está vazio.")
            conn.commit()
            # carregar_cadastro_contas.clear()  # Desabilitado - cache desligado
            return

        if 'Conta_OFX_Normalizada' in df.columns:
            df_final = df.copy()
            df_final.rename(columns=CADASTRO_COLS_DF_TO_DB, inplace=True)

            for col in db_cols:
                if col not in df_final.columns:
                    df_final[col] = None

            if 'Saldo_Inicial' in df_final.columns:
                df_final['Saldo_Inicial'] = df_final['Saldo_Inicial'].astype(str).str.replace(',', '.', regex=False)
                df_final['Saldo_Inicial'] = pd.to_numeric(df_final['Saldo_Inicial'], errors='coerce').fillna(0.0)
            if 'Data_Inicial_Saldo' in df_final.columns:
                df_final['Data_Inicial_Saldo'] = df_final['Data_Inicial_Saldo'].astype(object)

            df_final = df_final[db_cols]
            df_final.drop_duplicates(subset=['Codigo_Banco', 'Conta_OFX_Normalizada'], keep='last', inplace=True)
            
            # Anexa o dataframe limpo e processado
            df_final.to_sql(CADASTRO_CONTAS_TABLE, conn, if_exists='append', index=False)
            conn.commit()
            st.success("Cadastro salvo no banco de dados.")
            # carregar_cadastro_contas.clear()  # Desabilitado - cache desligado
        else:
            st.error("Erro: DataFrame de cadastro não possui a coluna 'Conta_OFX_Normalizada'.")

def salvar_contas_ofx_faltantes(df_ofx: pd.DataFrame, df_cadastro_atual: pd.DataFrame, df_bancos: pd.DataFrame):
    """
    Insere novas contas encontradas no OFX que não existem no cadastro.
    Esta função assume que as chaves 'Conta_OFX_Normalizada' em ambos os DataFrames
    já estão corretamente normalizadas pelo data_loader.
    """
    if df_ofx.empty or 'Conta_OFX_Normalizada' not in df_ofx.columns:
        return

    # As chaves já vêm normalizadas, então a comparação é direta.
    contas_ofx_unicas = df_ofx[['Banco_OFX', 'Conta_OFX_Normalizada']].drop_duplicates().copy()
    contas_ofx_unicas.rename(columns={'Banco_OFX': 'Codigo_Banco'}, inplace=True)

    if not df_cadastro_atual.empty:
        # Merge para identificar contas que já existem
        merged_df = pd.merge(
            contas_ofx_unicas, 
            df_cadastro_atual[['Codigo_Banco', 'Conta_OFX_Normalizada']], 
            on=['Codigo_Banco', 'Conta_OFX_Normalizada'], 
            how='left', 
            indicator=True
        )
        contas_novas = merged_df[merged_df['_merge'] == 'left_only'].copy()
        contas_novas.drop(columns='_merge', inplace=True)
    else:
        contas_novas = contas_ofx_unicas

    if not contas_novas.empty:
        st.info(f"Adicionando {len(contas_novas)} nova(s) conta(s) ao cadastro.")
        
        # Cria as colunas para as novas contas
        conta_str = contas_novas['Conta_OFX_Normalizada'].astype(str)
        contas_novas['Agencia'] = conta_str.str.slice(stop=4)
        contas_novas['Conta'] = conta_str.str.slice(start=4)
        contas_novas['Conta_Contabil'] = None
        contas_novas['Conta_Contabil_Negativo'] = None
        contas_novas['Saldo_Inicial'] = 0.0
        contas_novas['Data_Inicial_Saldo'] = None
        contas_novas['Path_Logo'] = os.path.join('logos', 'default.png')

        # Tenta adicionar o logo correto
        if not df_bancos.empty:
            df_bancos_logo = df_bancos[['codigo_banco', 'Path_Logo']].copy()
            df_bancos_logo.rename(columns={'Path_Logo': 'Logo_Correto'}, inplace=True)
            df_bancos_logo['codigo_banco'] = df_bancos_logo['codigo_banco'].astype(str).str.strip()
            
            contas_novas = pd.merge(contas_novas, df_bancos_logo, left_on='Codigo_Banco', right_on='codigo_banco', how='left')
            contas_novas['Path_Logo'] = np.where(contas_novas['Logo_Correto'].notna(), contas_novas['Logo_Correto'], contas_novas['Path_Logo'])
            contas_novas.drop(columns=['codigo_banco', 'Logo_Correto'], inplace=True, errors='ignore')

        df_final = contas_novas[['Agencia', 'Conta', 'Conta_OFX_Normalizada', 'Conta_Contabil', 'Conta_Contabil_Negativo', 'Saldo_Inicial', 'Data_Inicial_Saldo', 'Codigo_Banco', 'Path_Logo']]

        with get_db_connection() as conn:
            try:
                df_final.to_sql(CADASTRO_CONTAS_TABLE, conn, if_exists='append', index=False)
                st.success(f"Adicionadas {len(df_final)} novas contas ao cadastro.")
                # carregar_cadastro_contas.clear()  # Desabilitado - cache desligado
            except sqlite3.IntegrityError as e:
                st.warning(f"Aviso de Integridade do Banco de Dados: Uma ou mais contas já existiam e foram ignoradas. Detalhe: {e}")
            except Exception as e:
                st.error(f"Erro ao salvar novas contas no cadastro: {e}")

def excluir_conta_cadastro(conta_ofx_normalizada: str) -> bool:
    """Exclui uma conta específica do cadastro."""
    with get_db_connection() as conn:
        try:
            c = conn.cursor()
            c.execute(f"DELETE FROM {CADASTRO_CONTAS_TABLE} WHERE Conta_OFX_Normalizada = ?", (conta_ofx_normalizada,))
            conn.commit()
            if c.rowcount > 0:
                # carregar_cadastro_contas.clear()  # Desabilitado - cache desligado
                return True
            return False
        except Exception as e:
            st.error(f"Erro ao excluir conta do cadastro: {e}")
            return False

# ==============================================================================
# FUNÇÕES DE PLANO DE CONTAS
# ==============================================================================

@st.cache_data(show_spinner="Carregando plano de contas...")
def carregar_plano_contas() -> pd.DataFrame:
    """Carrega o plano de contas do banco de dados."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(f"SELECT * FROM {PLANO_CONTAS_TABLE} ORDER BY classificacao", conn)
            return df
    except Exception:
        return pd.DataFrame()

def salvar_plano_contas(df: pd.DataFrame):
    """Salva o DataFrame do plano de contas no banco de dados."""
    with get_db_connection() as conn:
        df.to_sql(PLANO_CONTAS_TABLE, conn, if_exists='replace', index=False)
        carregar_plano_contas.clear()

def excluir_conta_plano(codigo: str) -> bool:
    """Exclui uma conta do plano de contas pelo código."""
    with get_db_connection() as conn:
        try:
            c = conn.cursor()
            c.execute(f"DELETE FROM {PLANO_CONTAS_TABLE} WHERE codigo = ?", (codigo,))
            conn.commit()
            if c.rowcount > 0:
                carregar_plano_contas.clear()
                return True
            return False
        except Exception as e:
            st.error(f"Erro ao excluir conta do plano de contas: {e}")
            return False

# ==============================================================================
# FUNÇÕES DE LANÇAMENTOS CONTÁBEIS
# ==============================================================================
def salvar_lancamentos_contabeis(df: pd.DataFrame):
    """Salva o DataFrame de lançamentos contábeis no BD."""
    with get_db_connection() as conn:
        cols_map = {
            'Data Lançamento': 'data_lancamento',
            'Historico': 'historico',
            'Valor': 'valor',
            'Tipo Lancamento': 'tipo_lancamento',
            'ReduzDeb': 'reduz_deb',
            'NomeContaD': 'nome_conta_d',
            'ReduzCred': 'reduz_cred',
            'NomeContaC': 'nome_conta_c',
            'Origem': 'origem',
            'ID Lancamento': 'idlancamento'
        }
        
        # Garante que as colunas existam no DF antes de tentar acessá-las
        df_save = pd.DataFrame()
        for df_col, db_col in cols_map.items():
            if df_col in df.columns:
                df_save[db_col] = df[df_col]
            else:
                df_save[db_col] = None # Adiciona a coluna com nulos se não existir

        df_save.to_sql(LANCAMENTOS_CONTABEIS_TABLE, conn, if_exists='append', index=False)
        carregar_lancamentos_contabeis.clear()

def salvar_lancamentos_editados(df_editado: pd.DataFrame):
    """Atualiza os lançamentos contábeis no banco de dados a partir de um DataFrame editado."""
    if df_editado.empty:
        return

    with get_db_connection() as conn:
        c = conn.cursor()
        for _, row in df_editado.iterrows():
            try:
                # Converte a data para o formato YYYY-MM-DD
                data_formatada = pd.to_datetime(row['Data'], dayfirst=True).strftime('%Y-%m-%d')
                
                query = f"""
                    UPDATE {LANCAMENTOS_CONTABEIS_TABLE} SET
                        data_lancamento = ?,
                        historico = ?,
                        valor = ?,
                        reduz_deb = ?,
                        nome_conta_d = ?,
                        reduz_cred = ?,
                        nome_conta_c = ?,
                        origem = ?
                    WHERE id = ?
                """
                params = (
                    data_formatada,
                    row['Histórico'],
                    row['Valor'],
                    row['Débito'],
                    row['Nome Conta Débito'],
                    row['Crédito'],
                    row['Nome Conta Crédito'],
                    row['Origem'],
                    row['id']
                )
                c.execute(query, params)
            except Exception as e:
                st.error(f"Erro ao atualizar o lançamento com ID {row.get('id', 'N/A')}: {e}")
        conn.commit()
        carregar_lancamentos_contabeis.clear()

def excluir_lancamentos_por_ids(ids: list):
    """Exclui lançamentos contábeis do banco de dados com base em uma lista de IDs."""
    if not ids:
        return
    
    with get_db_connection() as conn:
        c = conn.cursor()
        try:
            # Cria uma string de placeholders (?, ?, ?) para a cláusula IN
            placeholders = ','.join('?' for _ in ids)
            query = f"DELETE FROM {LANCAMENTOS_CONTABEIS_TABLE} WHERE id IN ({placeholders})"
            c.execute(query, ids)
            conn.commit()
            st.success(f"{len(ids)} lançamento(s) excluído(s) com sucesso.")
            carregar_lancamentos_contabeis.clear() # Invalida o cache
            carregar_lancamentos_contabeis.clear()
        except Exception as e:
            st.error(f"Erro ao excluir lançamentos: {e}")

def excluir_lancamentos_por_idlancamentos(idlancamentos):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    placeholders = ','.join('?' for _ in idlancamentos)
    cursor.execute(f"DELETE FROM lancamentos_contabeis WHERE idlancamento IN ({placeholders})", idlancamentos) # Linha reinserida
    conn.commit()
    carregar_lancamentos_contabeis.clear() # Invalida o cache (agora no lugar certo)
    conn.close()
    return True



def salvar_partidas_lancamento(partidas):
    print(f"DEBUG: Partidas recebidas para salvar: {partidas}") # Linha de depuração
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Certificar-se de que a tabela existe
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lancamentos_contabeis (
            idlancamento TEXT,
            data_lancamento TEXT,
            historico TEXT,
            valor REAL,
            tipo_lancamento TEXT,
            reduz_deb TEXT,
            nome_conta_d TEXT,
            reduz_cred TEXT,
            nome_conta_c TEXT,
            origem TEXT
        )
    """)

    for partida in partidas:
        cursor.execute("""
            INSERT INTO lancamentos_contabeis (idlancamento, data_lancamento, historico, valor, tipo_lancamento, reduz_deb, nome_conta_d, reduz_cred, nome_conta_c, origem)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (partida['idlancamento'], partida['data_lancamento'], partida['historico'], partida['valor'], partida['tipo_lancamento'], partida['reduz_deb'], partida['nome_conta_d'], partida['reduz_cred'], partida['nome_conta_c'], partida['origem']))
    
    conn.commit()
    conn.close()
    carregar_lancamentos_contabeis.clear() # Invalida o cache
    return True


@st.cache_data(show_spinner="Carregando lançamentos contábeis...")
def carregar_lancamentos_contabeis() -> pd.DataFrame:
    """Carrega todos os lançamentos contábeis do banco de dados."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(f"SELECT * FROM {LANCAMENTOS_CONTABEIS_TABLE} ORDER BY data_lancamento DESC, id DESC", conn)
            print(f"DEBUG: carregar_lancamentos_contabeis - DataFrame carregado:\n{df.head()}") # Depuração
            return df
    except Exception as e:
        print(f"DEBUG: Erro ao carregar lançamentos contábeis: {e}") # Depuração
        return pd.DataFrame()

def limpar_lancamentos_contabeis():
    """Remove todos os registros da tabela de lançamentos contábeis."""
    with get_db_connection() as conn:
        try:
            conn.execute(f"DELETE FROM {LANCAMENTOS_CONTABEIS_TABLE}")
            conn.commit()
            st.success(f"Tabela '{LANCAMENTOS_CONTABEIS_TABLE}' limpa com sucesso.")
            carregar_lancamentos_contabeis.clear()
            return True
        except Exception as e:
            st.error(f"Erro ao tentar limpar os lançamentos contábeis: {e}")
            return False

# ==============================================================================
# FUNÇÕES DE HISTÓRICO
# ==============================================================================

def salvar_extrato_bancario_historico(df_ofx: pd.DataFrame):
    """Salva o DF do extrato bancário no histórico."""
    import hashlib

    with get_db_connection() as conn:
        c = conn.cursor()
        cols_map = {
            'ID Transacao': 'ID_Transacao',
            'Data Lançamento': 'Data_Lancamento',
            'Valor': 'Valor',
            'Descrição': 'Descricao',
            'Tipo': 'Tipo',
            'Banco_OFX': 'Banco_OFX',
            'Conta_OFX_Normalizada': 'Conta_OFX_Normalizada'
        }
        cols_to_save = [col for col in cols_map.keys() if col in df_ofx.columns]
        if not cols_to_save or 'ID Transacao' not in cols_to_save:
            return

        df_save = df_ofx[cols_to_save].copy()
        df_save.rename(columns=cols_map, inplace=True)

        if 'Valor' in df_save.columns:
            df_save['Valor'] = df_save['Valor'].astype(float)
        df_save['Data_Lancamento'] = pd.to_datetime(df_save['Data_Lancamento'], errors='coerce').dt.strftime('%Y-%m-%d')

        # IMPORTANTE: Gera um ID único para cada transação (hash MD5)
        # Evita problema de IDs duplicados do OFX da Caixa
        def gerar_id_unico(row, contador_duplicatas):
            """Gera ID único para a transação, adicionando contador se necessário"""
            chave_base = f"{row['ID_Transacao']}_{row['Data_Lancamento']}_{row['Valor']}_{row.get('Descricao', '')}_{row['Conta_OFX_Normalizada']}"

            # Se é a primeira ocorrência desta chave, usa hash simples
            if chave_base not in contador_duplicatas:
                contador_duplicatas[chave_base] = 0
                return hashlib.md5(chave_base.encode()).hexdigest()
            else:
                # Se já existe, incrementa contador e adiciona ao hash
                contador_duplicatas[chave_base] += 1
                chave_com_sufixo = f"{chave_base}_DUP{contador_duplicatas[chave_base]}"
                return hashlib.md5(chave_com_sufixo.encode()).hexdigest()

        # Verifica se a coluna ID_Unico já existe na tabela
        try:
            c.execute(f"PRAGMA table_info({EXTRATO_BANCARIO_TABLE})")
            columns_info = c.fetchall()
            has_id_unico = any(col[1] == 'ID_Unico' for col in columns_info)

            if not has_id_unico:
                # Adiciona a coluna ID_Unico se não existir
                c.execute(f"ALTER TABLE {EXTRATO_BANCARIO_TABLE} ADD COLUMN ID_Unico TEXT")
                conn.commit()

                # Remove constraint da PRIMARY KEY antiga (recriar tabela)
                st.warning("Reestruturando banco de dados para suportar IDs duplicados do OFX...")
                c.execute(f"DROP TABLE IF EXISTS {EXTRATO_BANCARIO_TABLE}_backup")
                c.execute(f"ALTER TABLE {EXTRATO_BANCARIO_TABLE} RENAME TO {EXTRATO_BANCARIO_TABLE}_backup")

                c.execute(f'''
                    CREATE TABLE {EXTRATO_BANCARIO_TABLE} (
                        ID_Unico TEXT PRIMARY KEY,
                        ID_Transacao TEXT,
                        Data_Lancamento DATE,
                        Valor REAL,
                        Descricao TEXT,
                        Tipo TEXT,
                        Banco_OFX TEXT,
                        Conta_OFX_Normalizada TEXT
                    )
                ''')

                # Migra dados antigos com lógica de ID único
                c.execute(f"SELECT * FROM {EXTRATO_BANCARIO_TABLE}_backup")
                old_data = c.fetchall()
                if old_data:
                    # Criar DataFrame temporário para aplicar a mesma lógica de ID único
                    df_old = pd.DataFrame(old_data, columns=['ID_Transacao', 'Data_Lancamento', 'Valor', 'Descricao', 'Tipo', 'Banco_OFX', 'Conta_OFX_Normalizada'])
                    contador_dup_old = {}
                    ids_unicos_old = []
                    for _, row in df_old.iterrows():
                        ids_unicos_old.append(gerar_id_unico(row, contador_dup_old))
                    df_old['ID_Unico'] = ids_unicos_old

                    for _, row in df_old.iterrows():
                        c.execute(f"""
                            INSERT OR IGNORE INTO {EXTRATO_BANCARIO_TABLE}
                            (ID_Unico, ID_Transacao, Data_Lancamento, Valor, Descricao, Tipo, Banco_OFX, Conta_OFX_Normalizada)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """, (row['ID_Unico'], row['ID_Transacao'], row['Data_Lancamento'], row['Valor'], row['Descricao'], row['Tipo'], row['Banco_OFX'], row['Conta_OFX_Normalizada']))

                c.execute(f"DROP TABLE {EXTRATO_BANCARIO_TABLE}_backup")
                conn.commit()
                st.success("Banco de dados reestruturado com sucesso!")

        except Exception as e:
            st.warning(f"Aviso na verificação de estrutura: {e}")

        # Adiciona ID_Unico ao DataFrame (detecta e diferencia duplicatas)
        contador_duplicatas = {}
        ids_unicos = []
        for _, row in df_save.iterrows():
            ids_unicos.append(gerar_id_unico(row, contador_duplicatas))
        df_save['ID_Unico'] = ids_unicos

        # Informa se houve transações duplicadas
        total_duplicatas = sum(1 for v in contador_duplicatas.values() if v > 0)
        if total_duplicatas > 0:
            st.info(f"ℹ️ {total_duplicatas} transação(ões) idêntica(s) detectada(s) e diferenciada(s) automaticamente.")

        # Reordena colunas para ID_Unico primeiro
        cols_order = ['ID_Unico'] + [col for col in df_save.columns if col != 'ID_Unico']
        df_save = df_save[cols_order]

        columns = ', '.join(df_save.columns)
        placeholders = ', '.join(['?' for _ in df_save.columns])
        insert_query = f"INSERT OR IGNORE INTO {EXTRATO_BANCARIO_TABLE} ({columns}) VALUES ({placeholders})"
        data_to_insert = [tuple(row) for row in df_save.values]

        try:
            c.executemany(insert_query, data_to_insert)
            conn.commit()

            try:
                carregar_extrato_bancario_historico.clear()
            except:
                pass  # Ignora erro se cache não disponível
            st.info(f"OK - {len(data_to_insert)} transações processadas para salvamento no histórico.")
        except Exception as e:
            st.error(f"Erro ao inserir dados no histórico do extrato: {e}")
            conn.rollback()

@st.cache_data(show_spinner="Carregando histórico do banco de dados...")
def carregar_extrato_bancario_historico(conta_ofx_normalizada: str, data_inicio: datetime.date, data_fim: datetime.date) -> pd.DataFrame:
    """Carrega o extrato bancário do histórico, filtrando por conta e período."""
    with get_db_connection() as conn:
        data_inicio_str = data_inicio.strftime('%Y-%m-%d')
        data_fim_str = data_fim.strftime('%Y-%m-%d')
        query = f"""
            SELECT 
                ID_Transacao AS \"ID Transacao\",
                Data_Lancamento AS \"Data Lançamento\",
                Valor, Descricao AS \"Descrição\",
                Tipo, Banco_OFX, Conta_OFX_Normalizada
            FROM {EXTRATO_BANCARIO_TABLE}
            WHERE Conta_OFX_Normalizada = ? AND Data_Lancamento BETWEEN ? AND ?
            ORDER BY Data_Lancamento ASC, ID_Transacao ASC;
        """
        params = (conta_ofx_normalizada, data_inicio_str, data_fim_str)
        df = pd.read_sql_query(query, conn, params=params)

        if not df.empty and 'Data Lançamento' in df.columns:
            df['Data Lançamento'] = pd.to_datetime(df['Data Lançamento'], errors='coerce').dt.date

        return df

def limpar_extrato_bancario_historico():
    """Remove todos os registros da tabela de histórico de extrato bancário."""
    with get_db_connection() as conn:
        try:
            conn.execute(f"DELETE FROM {EXTRATO_BANCARIO_TABLE}")
            conn.commit()
            st.success(f"Tabela '{EXTRATO_BANCARIO_TABLE}' limpa com sucesso.")
            carregar_extrato_bancario_historico.clear()
            return True
        except Exception as e:
            st.error(f"Erro ao tentar limpar o histórico de extrato: {e}")
            return False

# ==============================================================================
# FUNÇÕES DE CADASTRO DA EMPRESA
# ==============================================================================

@st.cache_data(show_spinner="Carregando dados da empresa...")
def carregar_empresa() -> dict:
    """Carrega os dados da empresa do banco de dados."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(f"SELECT * FROM {EMPRESA_TABLE} WHERE id = 1", conn)
            if df.empty:
                return {}
            return df.iloc[0].to_dict()
    except Exception:
        return {}

def salvar_empresa(dados_empresa: dict) -> bool:
    """Salva ou atualiza os dados da empresa no banco de dados."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            # Adiciona a data de cadastro se não existir
            if 'data_cadastro' not in dados_empresa or not dados_empresa['data_cadastro']:
                dados_empresa['data_cadastro'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Força o ID = 1 para garantir que só existe uma empresa
            dados_empresa['id'] = 1

            # Verifica se já existe um registro
            c.execute(f"SELECT id FROM {EMPRESA_TABLE} WHERE id = 1")
            existe = c.fetchone()

            if existe:
                # Atualiza o registro existente
                campos = ', '.join([f"{k} = ?" for k in dados_empresa.keys() if k != 'id'])
                valores = [v for k, v in dados_empresa.items() if k != 'id']
                valores.append(1)  # ID para o WHERE

                query = f"UPDATE {EMPRESA_TABLE} SET {campos} WHERE id = ?"
                c.execute(query, valores)
            else:
                # Insere novo registro
                campos = ', '.join(dados_empresa.keys())
                placeholders = ', '.join(['?' for _ in dados_empresa])
                valores = list(dados_empresa.values())

                query = f"INSERT INTO {EMPRESA_TABLE} ({campos}) VALUES ({placeholders})"
                c.execute(query, valores)

            conn.commit()

            # Limpar cache do Streamlit
            try:
                carregar_empresa.clear()
                st.cache_data.clear()
            except:
                pass

            return True
    except Exception as e:
        st.error(f"Erro ao salvar dados da empresa: {e}")
        return False

# ==============================================================================
# FUNÇÕES DE CADASTRO DE SÓCIOS
# ==============================================================================

@st.cache_data(show_spinner="Carregando sócios...")
def carregar_socios() -> pd.DataFrame:
    """Carrega todos os sócios da empresa do banco de dados."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(
                f"SELECT * FROM {SOCIOS_TABLE} WHERE empresa_id = 1 ORDER BY nome_completo",
                conn
            )
            return df
    except Exception:
        return pd.DataFrame()

def salvar_socio(dados_socio: dict) -> bool:
    """Salva um novo sócio no banco de dados."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            # Adiciona a data de cadastro
            dados_socio['data_cadastro'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            dados_socio['empresa_id'] = 1

            # Verifica se o CPF já existe
            c.execute(f"SELECT id FROM {SOCIOS_TABLE} WHERE cpf = ?", (dados_socio['cpf'],))
            existe = c.fetchone()

            if existe:
                st.error("Já existe um sócio cadastrado com este CPF!")
                return False

            # Insere novo sócio
            campos = ', '.join(dados_socio.keys())
            placeholders = ', '.join(['?' for _ in dados_socio])
            valores = list(dados_socio.values())

            query = f"INSERT INTO {SOCIOS_TABLE} ({campos}) VALUES ({placeholders})"
            c.execute(query, valores)
            conn.commit()
            carregar_socios.clear()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar sócio: {e}")
        return False

def atualizar_socio(id_socio: int, dados_socio: dict) -> bool:
    """Atualiza os dados de um sócio existente."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            # Remove o id dos dados para não atualizar
            dados_socio_update = {k: v for k, v in dados_socio.items() if k != 'id'}

            campos = ', '.join([f"{k} = ?" for k in dados_socio_update.keys()])
            valores = list(dados_socio_update.values())
            valores.append(id_socio)

            query = f"UPDATE {SOCIOS_TABLE} SET {campos} WHERE id = ?"
            c.execute(query, valores)
            conn.commit()
            carregar_socios.clear()
            return True
    except Exception as e:
        st.error(f"Erro ao atualizar sócio: {e}")
        return False

def excluir_socio(id_socio: int) -> bool:
    """Exclui um sócio do banco de dados."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(f"DELETE FROM {SOCIOS_TABLE} WHERE id = ?", (id_socio,))
            conn.commit()
            carregar_socios.clear()
            return True
    except Exception as e:
        st.error(f"Erro ao excluir sócio: {e}")
        return False

# ==============================================================================
# FUNÇÕES DE GERENCIAMENTO DE LOGOTIPOS
# ==============================================================================

@st.cache_data(show_spinner="Carregando logotipos...")
def carregar_logotipos() -> pd.DataFrame:
    """Carrega todos os logotipos da empresa do banco de dados."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(
                f"SELECT * FROM {LOGOTIPOS_TABLE} WHERE empresa_id = 1 ORDER BY logo_principal DESC, data_upload DESC",
                conn
            )
            return df
    except Exception:
        return pd.DataFrame()

def salvar_logotipo(nome_arquivo: str, descricao: str, caminho_arquivo: str, logo_principal: bool = False) -> bool:
    """Salva um novo logotipo no banco de dados."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            # Se for logo principal, remove o flag de outros logos
            if logo_principal:
                c.execute(f"UPDATE {LOGOTIPOS_TABLE} SET logo_principal = FALSE WHERE empresa_id = 1")

            dados_logo = {
                'empresa_id': 1,
                'nome_arquivo': nome_arquivo,
                'descricao': descricao,
                'caminho_arquivo': caminho_arquivo,
                'logo_principal': logo_principal,
                'data_upload': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }

            campos = ', '.join(dados_logo.keys())
            placeholders = ', '.join(['?' for _ in dados_logo])
            valores = list(dados_logo.values())

            query = f"INSERT INTO {LOGOTIPOS_TABLE} ({campos}) VALUES ({placeholders})"
            c.execute(query, valores)
            conn.commit()
            carregar_logotipos.clear()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar logotipo: {e}")
        return False

def definir_logo_principal(id_logo: int) -> bool:
    """Define um logotipo como principal."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            # Remove flag de todos
            c.execute(f"UPDATE {LOGOTIPOS_TABLE} SET logo_principal = FALSE WHERE empresa_id = 1")
            # Define o selecionado como principal
            c.execute(f"UPDATE {LOGOTIPOS_TABLE} SET logo_principal = TRUE WHERE id = ?", (id_logo,))
            conn.commit()
            carregar_logotipos.clear()
            return True
    except Exception as e:
        st.error(f"Erro ao definir logo principal: {e}")
        return False

def excluir_logotipo(id_logo: int, caminho_arquivo: str) -> bool:
    """Exclui um logotipo do banco de dados e do disco."""
    try:
        # Remove do disco
        if os.path.exists(caminho_arquivo):
            os.remove(caminho_arquivo)

        # Remove do banco
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(f"DELETE FROM {LOGOTIPOS_TABLE} WHERE id = ?", (id_logo,))
            conn.commit()
            carregar_logotipos.clear()
            return True
    except Exception as e:
        st.error(f"Erro ao excluir logotipo: {e}")
        return False

def obter_logo_principal() -> str:
    """Retorna o caminho do logotipo principal da empresa."""
    try:
        df_logos = carregar_logotipos()
        if not df_logos.empty:
            logo_principal = df_logos[df_logos['logo_principal'] == True]
            if not logo_principal.empty:
                return logo_principal.iloc[0]['caminho_arquivo']
            else:
                # Se não tem principal, retorna o primeiro
                return df_logos.iloc[0]['caminho_arquivo']
        return 'logos/default.png'
    except:
        return 'logos/default.png'


# ==============================================================================
# FUNÇÕES DE PARCELAMENTOS
# ==============================================================================

@st.cache_data(show_spinner="Carregando parcelamentos...")
def carregar_parcelamentos() -> pd.DataFrame:
    """Carrega todos os parcelamentos do banco de dados."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(
                f"SELECT * FROM {PARCELAMENTOS_TABLE} ORDER BY numero_parcelamento",
                conn
            )
            return df
    except Exception:
        return pd.DataFrame()


def salvar_parcelamento(dados: dict) -> int:
    """Salva um novo parcelamento e retorna o ID inserido."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            dados['data_cadastro'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            campos = ', '.join(dados.keys())
            placeholders = ', '.join(['?' for _ in dados])
            valores = list(dados.values())

            query = f"INSERT INTO {PARCELAMENTOS_TABLE} ({campos}) VALUES ({placeholders})"
            c.execute(query, valores)
            conn.commit()

            parcelamento_id = c.lastrowid
            carregar_parcelamentos.clear()
            return parcelamento_id
    except sqlite3.IntegrityError:
        st.error("Já existe um parcelamento com este número!")
        return None
    except Exception as e:
        st.error(f"Erro ao salvar parcelamento: {e}")
        return None


def atualizar_parcelamento(parcelamento_id: int, dados: dict) -> bool:
    """Atualiza os dados de um parcelamento existente."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            campos = ', '.join([f"{k} = ?" for k in dados.keys()])
            valores = list(dados.values())
            valores.append(parcelamento_id)

            query = f"UPDATE {PARCELAMENTOS_TABLE} SET {campos} WHERE id = ?"
            c.execute(query, valores)
            conn.commit()
            carregar_parcelamentos.clear()
            return True
    except Exception as e:
        st.error(f"Erro ao atualizar parcelamento: {e}")
        return False


def excluir_parcelamento(parcelamento_id: int) -> bool:
    """Exclui um parcelamento e todos os seus dados relacionados."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            # Exclui dados relacionados primeiro
            c.execute(f"DELETE FROM {PARCELAMENTO_DEBITOS_TABLE} WHERE parcelamento_id = ?", (parcelamento_id,))
            c.execute(f"DELETE FROM {PARCELAMENTO_PARCELAS_TABLE} WHERE parcelamento_id = ?", (parcelamento_id,))
            c.execute(f"DELETE FROM {PARCELAMENTO_PAGAMENTOS_TABLE} WHERE parcelamento_id = ?", (parcelamento_id,))
            # Exclui o parcelamento
            c.execute(f"DELETE FROM {PARCELAMENTOS_TABLE} WHERE id = ?", (parcelamento_id,))
            conn.commit()
            carregar_parcelamentos.clear()
            return True
    except Exception as e:
        st.error(f"Erro ao excluir parcelamento: {e}")
        return False


def carregar_parcelamento_por_id(parcelamento_id: int) -> dict:
    """Carrega um parcelamento específico pelo ID."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(
                f"SELECT * FROM {PARCELAMENTOS_TABLE} WHERE id = ?",
                conn, params=(parcelamento_id,)
            )
            if df.empty:
                return {}
            return df.iloc[0].to_dict()
    except Exception:
        return {}


# ==============================================================================
# FUNÇÕES DE DÉBITOS DO PARCELAMENTO
# ==============================================================================

def carregar_debitos_parcelamento(parcelamento_id: int) -> pd.DataFrame:
    """Carrega os débitos de um parcelamento específico."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(
                f"SELECT * FROM {PARCELAMENTO_DEBITOS_TABLE} WHERE parcelamento_id = ? ORDER BY data_vencimento",
                conn, params=(parcelamento_id,)
            )
            return df
    except Exception:
        return pd.DataFrame()


def salvar_debitos_parcelamento(parcelamento_id: int, lista_debitos: list) -> bool:
    """Salva a lista de débitos de um parcelamento (substitui os existentes)."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            # Remove débitos antigos
            c.execute(f"DELETE FROM {PARCELAMENTO_DEBITOS_TABLE} WHERE parcelamento_id = ?", (parcelamento_id,))

            # Insere novos débitos
            for debito in lista_debitos:
                debito['parcelamento_id'] = parcelamento_id
                campos = ', '.join(debito.keys())
                placeholders = ', '.join(['?' for _ in debito])
                valores = list(debito.values())

                query = f"INSERT INTO {PARCELAMENTO_DEBITOS_TABLE} ({campos}) VALUES ({placeholders})"
                c.execute(query, valores)

            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar débitos do parcelamento: {e}")
        return False


# ==============================================================================
# FUNÇÕES DE PARCELAS
# ==============================================================================

def carregar_parcelas_parcelamento(parcelamento_id: int) -> pd.DataFrame:
    """Carrega as parcelas de um parcelamento específico."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(
                f"SELECT * FROM {PARCELAMENTO_PARCELAS_TABLE} WHERE parcelamento_id = ? ORDER BY numero_parcela",
                conn, params=(parcelamento_id,)
            )
            return df
    except Exception:
        return pd.DataFrame()


def salvar_parcelas_parcelamento(parcelamento_id: int, lista_parcelas: list) -> bool:
    """Salva a lista de parcelas de um parcelamento (substitui as existentes)."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            # Remove parcelas antigas
            c.execute(f"DELETE FROM {PARCELAMENTO_PARCELAS_TABLE} WHERE parcelamento_id = ?", (parcelamento_id,))

            # Insere novas parcelas
            for parcela in lista_parcelas:
                parcela['parcelamento_id'] = parcelamento_id
                campos = ', '.join(parcela.keys())
                placeholders = ', '.join(['?' for _ in parcela])
                valores = list(parcela.values())

                query = f"INSERT INTO {PARCELAMENTO_PARCELAS_TABLE} ({campos}) VALUES ({placeholders})"
                c.execute(query, valores)

            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar parcelas do parcelamento: {e}")
        return False


def atualizar_parcela(parcela_id: int, dados: dict) -> bool:
    """Atualiza os dados de uma parcela específica."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            campos = ', '.join([f"{k} = ?" for k in dados.keys()])
            valores = list(dados.values())
            valores.append(parcela_id)

            query = f"UPDATE {PARCELAMENTO_PARCELAS_TABLE} SET {campos} WHERE id = ?"
            c.execute(query, valores)
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao atualizar parcela: {e}")
        return False


# ==============================================================================
# FUNÇÕES DE PAGAMENTOS DO PARCELAMENTO
# ==============================================================================

def carregar_pagamentos_parcelamento(parcelamento_id: int) -> pd.DataFrame:
    """Carrega os pagamentos de um parcelamento específico."""
    try:
        with get_db_connection() as conn:
            df = pd.read_sql_query(
                f"SELECT * FROM {PARCELAMENTO_PAGAMENTOS_TABLE} WHERE parcelamento_id = ? ORDER BY data_pagamento DESC",
                conn, params=(parcelamento_id,)
            )
            return df
    except Exception:
        return pd.DataFrame()


def salvar_pagamento_parcelamento(parcelamento_id: int, dados_pagamento: dict) -> bool:
    """Salva um novo pagamento de parcelamento."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            dados_pagamento['parcelamento_id'] = parcelamento_id

            campos = ', '.join(dados_pagamento.keys())
            placeholders = ', '.join(['?' for _ in dados_pagamento])
            valores = list(dados_pagamento.values())

            query = f"INSERT INTO {PARCELAMENTO_PAGAMENTOS_TABLE} ({campos}) VALUES ({placeholders})"
            c.execute(query, valores)
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao salvar pagamento: {e}")
        return False


def atualizar_conciliacao_pagamento(pagamento_id: int, id_transacao_banco: str) -> bool:
    """Marca um pagamento como conciliado com uma transação bancária."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            query = f"""
                UPDATE {PARCELAMENTO_PAGAMENTOS_TABLE}
                SET conciliado = TRUE,
                    id_transacao_banco = ?,
                    data_conciliacao = ?
                WHERE id = ?
            """
            c.execute(query, (id_transacao_banco, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), pagamento_id))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao conciliar pagamento: {e}")
        return False


def excluir_pagamento_parcelamento(pagamento_id: int) -> bool:
    """Exclui um pagamento do parcelamento."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()
            c.execute(f"DELETE FROM {PARCELAMENTO_PAGAMENTOS_TABLE} WHERE id = ?", (pagamento_id,))
            conn.commit()
            return True
    except Exception as e:
        st.error(f"Erro ao excluir pagamento: {e}")
        return False


def atualizar_saldo_parcelamento(parcelamento_id: int) -> bool:
    """Recalcula e atualiza o saldo devedor e contadores do parcelamento."""
    try:
        with get_db_connection() as conn:
            c = conn.cursor()

            # Conta parcelas por situação
            c.execute(f"""
                SELECT
                    SUM(CASE WHEN situacao = 'Paga' THEN 1 ELSE 0 END) as pagas,
                    SUM(CASE WHEN situacao = 'Devedora' THEN 1 ELSE 0 END) as vencidas,
                    SUM(CASE WHEN situacao = 'A vencer' THEN 1 ELSE 0 END) as a_vencer,
                    SUM(CASE WHEN situacao != 'Paga' THEN COALESCE(saldo_atualizado, valor_originario, 0) ELSE 0 END) as saldo
                FROM {PARCELAMENTO_PARCELAS_TABLE}
                WHERE parcelamento_id = ?
            """, (parcelamento_id,))

            result = c.fetchone()
            if result:
                pagas, vencidas, a_vencer, saldo = result

                c.execute(f"""
                    UPDATE {PARCELAMENTOS_TABLE}
                    SET qtd_pagas = ?,
                        qtd_vencidas = ?,
                        qtd_a_vencer = ?,
                        saldo_devedor = ?
                    WHERE id = ?
                """, (pagas or 0, vencidas or 0, a_vencer or 0, saldo or 0, parcelamento_id))

                conn.commit()
                carregar_parcelamentos.clear()
            return True
    except Exception as e:
        st.error(f"Erro ao atualizar saldo do parcelamento: {e}")
        return False