# conciliacao.py
import pandas as pd
import streamlit as st
from typing import Tuple
from datetime import timedelta
import numpy as np
import datetime
import uuid

# CORREÇÃO: Importação Absoluta
from utils import normalizar_numero
from db_manager import carregar_extrato_bancario_historico, carregar_plano_contas, carregar_lancamentos_contabeis


# ==============================================================================
# CONFIGURAÇÃO DE PASSAGENS DE CONCILIAÇÃO
# ==============================================================================
PASSAGES_CONFIG = [
    # 1. Passagem Exata: Valor, Data e Conta Exatas
    {'name': 'Passagem 1: Valor, Data e Conta EXATAS',
     'tolerance': 0.00, 'date_tolerance_days': 0, 'use_value_key': False},

    # 2. Passagem Tolerante: Valor Exato e Conta Exata (com Data +/- 1 dia)
    {'name': 'Passagem 2: Valor Exato e Conta (Data ± 1 dia)',
     'tolerance': 0.00, 'date_tolerance_days': 1, 'use_value_key': False},

    # 3. Passagem Tolerante na Data: Valor e Conta (com Data +/- 5 dias)
    {'name': 'Passagem 3: Valor Exato e Conta (Data ± 5 dias)',
     'tolerance': 0.00, 'date_tolerance_days': 5, 'use_value_key': False},

    # 4. Passagem de Tolerância de Valor: Data e Conta Exatas (Valor +/- 0.05)
    {'name': 'Passagem 4: Data e Conta Exatas (Valor ± 0.05)',
     'tolerance': 0.05, 'date_tolerance_days': 0, 'use_value_key': False},

    # ... Adicione suas outras passagens aqui ...
]


# ==============================================================================
# 1. VINCULAÇÃO DE CONTAS (PRÉ-CONCILIAÇÃO)
# ==============================================================================

@st.cache_data(show_spinner="Vinculando contas contábeis ao extrato...")
def vincular_contas_ao_extrato(df_extrato: pd.DataFrame, df_contas: pd.DataFrame) -> pd.DataFrame:
    """Vincula o extrato bancário com o cadastro de contas para adicionar a Conta Contábil."""
    if df_contas.empty or 'Conta_OFX_Normalizada' not in df_extrato.columns:
        return df_extrato.assign(Conta_Contábil_Vinculada='N/A')

    df_extrato_copy = df_extrato.copy()

    # Garante que a coluna de merge esteja presente no df_contas
    if 'Conta_OFX_Normalizada' not in df_contas.columns:
        st.error("A coluna 'Conta_OFX_Normalizada' não está no Cadastro de Contas.")
        return df_extrato_copy.assign(Conta_Contábil_Vinculada='N/A')

    df_map = df_contas[['Conta_OFX_Normalizada', 'Conta Contábil']].drop_duplicates()
    mapa_contas = df_map.set_index('Conta_OFX_Normalizada')['Conta Contábil'].to_dict()

    df_extrato_copy['Conta_Contábil_Vinculada'] = df_extrato_copy['Conta_OFX_Normalizada'].map(mapa_contas).fillna(
        'N/A')

    return df_extrato_copy


# ==============================================================================
# 2. LÓGICA DE MATCHING (FUNÇÕES INTERNAS)
# ==============================================================================

def _find_match(row, df_contabil_helper, tolerance: float, date_tolerance_days: int) -> pd.Series:
    """Função interna para encontrar uma única correspondência no extrato contábil."""

    valor_match = -row['Valor']
    conta_match = row['Conta_Contábil_Vinculada']
    data_match = row['Data Lançamento']

    df_filtered_conta = df_contabil_helper[df_contabil_helper['Conta Contábil'] == conta_match]

    if df_filtered_conta.empty:
        return pd.Series([False, None])

    df_filtered_valor = df_filtered_conta[
        (df_filtered_conta['Valor'].abs() >= abs(valor_match) - tolerance) &
        (df_filtered_conta['Valor'].abs() <= abs(valor_match) + tolerance)
        ].copy()

    if df_filtered_valor.empty:
        return pd.Series([False, None])

    min_date = data_match - timedelta(days=date_tolerance_days)
    max_date = data_match + timedelta(days=date_tolerance_days)

    df_filtered_data = df_filtered_valor[
        (df_filtered_valor['Data'] >= min_date) &
        (df_filtered_valor['Data'] <= max_date)
        ]

    if df_filtered_data.empty:
        return pd.Series([False, None])

    if len(df_filtered_data) > 1:
        df_filtered_data['diff_days'] = (df_filtered_data['Data'] - data_match).apply(lambda x: abs(x.days))
        match = df_filtered_data.sort_values(by='diff_days').iloc[0]
    else:
        match = df_filtered_data.iloc[0]

    return pd.Series([True, match['ID Contabil']])


def _executar_passagem(df_ofx, df_contabil_helper, df_contabil_raw, pass_info: dict) -> Tuple[
    pd.DataFrame, pd.DataFrame]:
    """Executa uma única passagem de conciliação."""

    st.write(f"--- Executando: {pass_info['name']} ---")

    df_unmatched = df_ofx[df_ofx['Conciliado_Contábil'] == 'Não'].copy()

    if df_unmatched.empty:
        return df_ofx, df_contabil_raw

    results = df_unmatched.apply(
        lambda row: _find_match(row, df_contabil_helper, pass_info['tolerance'], pass_info['date_tolerance_days']),
        axis=1,
        result_type='expand'
    )
    results.columns = ['Match_Found', 'Matched_ID']

    matched_ofx_indices = results[results['Match_Found']].index
    matched_contabil_ids = results[results['Match_Found']]['Matched_ID'].dropna().unique().astype(int)

    if matched_ofx_indices.empty:
        st.info(f"0 matches encontrados na {pass_info['name']}.")
        return df_ofx, df_contabil_raw

    # Marca no DF OFX
    df_ofx.loc[matched_ofx_indices, 'Conciliado_Contábil'] = 'Sim'
    df_ofx.loc[matched_ofx_indices, 'ID_Contabil_Conciliado'] = results.loc[matched_ofx_indices, 'Matched_ID']
    df_ofx.loc[matched_ofx_indices, 'Passagem_Conciliacao'] = pass_info['name']

    # Marca no DF Contábil
    df_contabil_raw.loc[
        df_contabil_raw['ID Contabil'].isin(matched_contabil_ids),
        'Conciliado_OFX'
    ] = 'Sim'

    st.success(f"{len(matched_ofx_indices)} matches encontrados na {pass_info['name']}.")
    return df_ofx, df_contabil_raw


# ==============================================================================
# 3. FUNÇÃO PRINCIPAL DE CONCILIAÇÃO
# ==============================================================================

@st.cache_data(show_spinner="Executando Conciliação Multi-Pass...")
def conciliar_extratos(df_extrato_vinculado: pd.DataFrame, df_contabil: pd.DataFrame) -> Tuple[
    pd.DataFrame, pd.DataFrame]:
    """Executa todas as passagens de conciliação definidas."""

    # 1. Inicialização dos DataFrames de resultado
    df_ofx_conc = df_extrato_vinculado.copy()
    df_contabil_conc = df_contabil.copy()

    # Adiciona colunas de controle
    if 'Conciliado_Contábil' not in df_ofx_conc.columns:
        df_ofx_conc['Conciliado_Contábil'] = 'Não'
        df_ofx_conc['ID_Contabil_Conciliado'] = np.nan
        df_ofx_conc['Passagem_Conciliacao'] = 'N/A'

    if 'Conciliado_OFX' not in df_contabil_conc.columns:
        df_contabil_conc['Conciliado_OFX'] = 'Não'

    # Garante que ID Contabil seja numérico e único
    df_contabil_conc['ID Contabil'] = df_contabil_conc['ID Contabil'].fillna(
        pd.Series(range(1, len(df_contabil_conc) + 1))
    ).astype(int)

    # 2. Criação de um helper DF
    df_contabil_helper = df_contabil_conc.copy()

    # 3. Execução das Passagens
    for pass_info in PASSAGES_CONFIG:
        # Passa apenas as transações contábeis que ainda não foram conciliadas
        df_ofx_conc, df_contabil_conc = _executar_passagem(
            df_ofx_conc,
            df_contabil_helper[df_contabil_helper['Conciliado_OFX'] == 'Não'],
            df_contabil_conc,
            pass_info
        )

    st.success("Conciliação Multi-Pass concluída!")
    return df_ofx_conc, df_contabil_conc

# ==============================================================================
# 4. LÓGICA DE CONCILIAÇÃO DE SALDO NEGATIVO
# ==============================================================================

def gerar_lancamentos_saldo_negativo(conta_selecionada_row: pd.Series, data_inicio: datetime.date, data_fim: datetime.date) -> pd.DataFrame:
    """
    Analisa o saldo diário de uma conta e gera lançamentos de ajuste para cobrir saldos negativos.
    """
    # 1. Validar informações da conta
    conta_contabil_principal = conta_selecionada_row.get('Conta Contábil')
    conta_contabil_negativo = conta_selecionada_row.get('Conta Contábil (-)')
    
    if not conta_contabil_principal or pd.isna(conta_contabil_principal) or not conta_contabil_negativo or pd.isna(conta_contabil_negativo):
        st.error("A conta bancária selecionada não possui a 'Conta Contábil' e/ou a 'Conta Contábil (-)' preenchidas no cadastro. Verifique o Menu 1.1.")
        return pd.DataFrame()

    plano_contas = carregar_plano_contas()
    mapa_nomes_contas = plano_contas.set_index('codigo')['descricao'].to_dict()

    nome_conta_principal = mapa_nomes_contas.get(str(int(conta_contabil_principal)), "NOME NÃO ENCONTRADO")
    nome_conta_negativo = mapa_nomes_contas.get(str(int(conta_contabil_negativo)), "NOME NÃO ENCONTRADO")

    # 2. Obter dados para cálculo do saldo
    conta_ofx = conta_selecionada_row['Conta_OFX_Normalizada']
    saldo_inicial_cadastro = conta_selecionada_row.get('Saldo Inicial', 0.0)
    data_saldo_inicial_str = conta_selecionada_row.get('Data Inicial Saldo')

    data_saldo_inicial = datetime.datetime.strptime(data_saldo_inicial_str, '%d%m%Y').date() if data_saldo_inicial_str and pd.notna(data_saldo_inicial_str) else None

    # Define o início da busca de transações
    data_inicio_extrato = data_saldo_inicial if data_saldo_inicial else datetime.date(2000, 1, 1)
    
    # Carrega todas as transações até a data fim da análise
    df_transacoes = carregar_extrato_bancario_historico(conta_ofx, data_inicio_extrato, data_fim)
    
    # 3. Calcular saldos diários
    if not df_transacoes.empty:
        df_transacoes['Data Lançamento'] = pd.to_datetime(df_transacoes['Data Lançamento'])
        movimento_diario = df_transacoes.groupby(df_transacoes['Data Lançamento'].dt.date)['Valor'].sum()
    else:
        movimento_diario = pd.Series(dtype=float)
    
    # Cria um range de datas completo para garantir que todos os dias sejam considerados
    start_date_calc = data_saldo_inicial if data_saldo_inicial else (min(movimento_diario.index) if not movimento_diario.empty else data_inicio)
    
    all_dates = pd.date_range(start=start_date_calc, end=data_fim, freq='D')
    
    df_saldos = pd.DataFrame(index=all_dates)
    df_saldos['movimento'] = movimento_diario
    df_saldos['movimento'].fillna(0, inplace=True)
    
    # Calcula o saldo acumulado
    df_saldos['saldo_final'] = saldo_inicial_cadastro + df_saldos['movimento'].cumsum()

    # 4. Gerar lançamentos de ajuste
    lancamentos_propostos = []
    saldo_provisionado_anterior = 0

    # Calcula a provisão existente antes do período de análise do usuário
    df_antes_periodo = df_saldos[df_saldos.index.date < data_inicio]
    if not df_antes_periodo.empty:
        saldo_final_anterior = df_antes_periodo.iloc[-1]['saldo_final']
        if saldo_final_anterior < 0:
            saldo_provisionado_anterior = abs(saldo_final_anterior)

    # Itera sobre os dias no período selecionado pelo usuário
    df_periodo_analise = df_saldos[(df_saldos.index.date >= data_inicio) & (df_saldos.index.date <= data_fim)]
    
    for data, row in df_periodo_analise.iterrows():
        saldo_do_dia = row['saldo_final']
        valor_a_provisionar_hoje = abs(saldo_do_dia) if saldo_do_dia < 0 else 0
            
        ajuste_necessario = valor_a_provisionar_hoje - saldo_provisionado_anterior
        
        if round(ajuste_necessario, 2) != 0:
            idlanc = str(uuid.uuid4())
            data_lanc_str = data.strftime('%Y-%m-%d')
            valor_ajuste = abs(ajuste_necessario)

            if ajuste_necessario > 0: # Provisão ou aumento da provisão
                lanc = {
                    'idlancamento': idlanc, 'data_lancamento': data_lanc_str,
                    'historico': f"Provisão para cobertura de saldo negativo em {data.strftime('%d/%m/%Y')}",
                    'valor': valor_ajuste, 'tipo_lancamento': 'Ajuste',
                    'reduz_deb': str(int(conta_contabil_principal)), 'nome_conta_d': nome_conta_principal,
                    'reduz_cred': str(int(conta_contabil_negativo)), 'nome_conta_c': nome_conta_negativo,
                    'origem': 'conta negativa'
                }
                lancamentos_propostos.append(lanc)
            
            else: # Reversão ou diminuição da provisão
                lanc = {
                    'idlancamento': idlanc, 'data_lancamento': data_lanc_str,
                    'historico': f"Reversão de provisão de saldo negativo em {data.strftime('%d/%m/%Y')}",
                    'valor': valor_ajuste, 'tipo_lancamento': 'Ajuste',
                    'reduz_deb': str(int(conta_contabil_negativo)), 'nome_conta_d': nome_conta_negativo,
                    'reduz_cred': str(int(conta_contabil_principal)), 'nome_conta_c': nome_conta_principal,
                    'origem': 'conta negativa'
                }
                lancamentos_propostos.append(lanc)

        saldo_provisionado_anterior = valor_a_provisionar_hoje

    if not lancamentos_propostos:
        st.info("Nenhum ajuste de saldo negativo foi necessário no período.")
        return pd.DataFrame()

    return pd.DataFrame(lancamentos_propostos)


def gerar_lancamentos_saldo_negativo_contabil(
    conta_contabil_principal: str,
    nome_conta_principal: str,
    conta_contabil_negativo: str,
    nome_conta_negativo: str,
    saldo_inicial: float,
    data_saldo_inicial: datetime.date,
    data_inicio: datetime.date,
    data_fim: datetime.date
) -> pd.DataFrame:
    """
    Analisa o saldo diário de uma conta contábil (baseado nos lançamentos contábeis)
    e gera lançamentos de ajuste para cobrir saldos negativos.

    Similar a gerar_lancamentos_saldo_negativo, mas usa lançamentos contábeis
    em vez de extratos bancários OFX.
    """
    # 1. Carregar lançamentos contábeis
    df_lancamentos = carregar_lancamentos_contabeis()

    if df_lancamentos.empty:
        st.warning("Não há lançamentos contábeis cadastrados.")
        return pd.DataFrame()

    # Converter data_lancamento para datetime
    df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'], errors='coerce')

    # Filtrar lançamentos que envolvem a conta principal (até data_fim)
    mask_debito = df_lancamentos['reduz_deb'] == conta_contabil_principal
    mask_credito = df_lancamentos['reduz_cred'] == conta_contabil_principal
    mask_data = df_lancamentos['data_lancamento'].dt.date <= data_fim

    if data_saldo_inicial:
        mask_data = mask_data & (df_lancamentos['data_lancamento'].dt.date >= data_saldo_inicial)

    df_conta = df_lancamentos[(mask_debito | mask_credito) & mask_data].copy()

    # 2. Calcular movimento diário
    # Para contas de Ativo: Débito aumenta (+), Crédito diminui (-)
    def calcular_movimento(row):
        if row['reduz_deb'] == conta_contabil_principal:
            return row['valor']  # Débito aumenta saldo
        else:
            return -row['valor']  # Crédito diminui saldo

    if not df_conta.empty:
        df_conta['movimento'] = df_conta.apply(calcular_movimento, axis=1)
        movimento_diario = df_conta.groupby(df_conta['data_lancamento'].dt.date)['movimento'].sum()
    else:
        movimento_diario = pd.Series(dtype=float)

    # 3. Criar range de datas completo
    start_date_calc = data_saldo_inicial if data_saldo_inicial else (min(movimento_diario.index) if not movimento_diario.empty else data_inicio)

    all_dates = pd.date_range(start=start_date_calc, end=data_fim, freq='D')

    df_saldos = pd.DataFrame(index=all_dates)
    df_saldos['movimento'] = movimento_diario
    df_saldos['movimento'].fillna(0, inplace=True)

    # Calcular saldo acumulado
    df_saldos['saldo_final'] = saldo_inicial + df_saldos['movimento'].cumsum()

    # 4. Gerar lançamentos de ajuste
    lancamentos_propostos = []
    saldo_provisionado_anterior = 0

    # Calcular provisão existente antes do período de análise
    df_antes_periodo = df_saldos[df_saldos.index.date < data_inicio]
    if not df_antes_periodo.empty:
        saldo_final_anterior = df_antes_periodo.iloc[-1]['saldo_final']
        if saldo_final_anterior < 0:
            saldo_provisionado_anterior = abs(saldo_final_anterior)

    # Iterar sobre dias no período selecionado
    df_periodo_analise = df_saldos[(df_saldos.index.date >= data_inicio) & (df_saldos.index.date <= data_fim)]

    for data, row in df_periodo_analise.iterrows():
        saldo_do_dia = row['saldo_final']
        valor_a_provisionar_hoje = abs(saldo_do_dia) if saldo_do_dia < 0 else 0

        ajuste_necessario = valor_a_provisionar_hoje - saldo_provisionado_anterior

        if round(ajuste_necessario, 2) != 0:
            idlanc = str(uuid.uuid4())
            data_lanc_str = data.strftime('%Y-%m-%d')
            valor_ajuste = abs(ajuste_necessario)

            if ajuste_necessario > 0:  # Provisão ou aumento da provisão
                lanc = {
                    'idlancamento': idlanc, 'data_lancamento': data_lanc_str,
                    'historico': f"Provisão para cobertura de saldo negativo contábil em {data.strftime('%d/%m/%Y')}",
                    'valor': valor_ajuste, 'tipo_lancamento': 'Ajuste',
                    'reduz_deb': conta_contabil_principal, 'nome_conta_d': nome_conta_principal,
                    'reduz_cred': conta_contabil_negativo, 'nome_conta_c': nome_conta_negativo,
                    'origem': 'conta contabil negativa'
                }
                lancamentos_propostos.append(lanc)
            else:  # Reversão ou diminuição da provisão
                lanc = {
                    'idlancamento': idlanc, 'data_lancamento': data_lanc_str,
                    'historico': f"Reversão de provisão de saldo negativo contábil em {data.strftime('%d/%m/%Y')}",
                    'valor': valor_ajuste, 'tipo_lancamento': 'Ajuste',
                    'reduz_deb': conta_contabil_negativo, 'nome_conta_d': nome_conta_negativo,
                    'reduz_cred': conta_contabil_principal, 'nome_conta_c': nome_conta_principal,
                    'origem': 'conta contabil negativa'
                }
                lancamentos_propostos.append(lanc)

        saldo_provisionado_anterior = valor_a_provisionar_hoje

    if not lancamentos_propostos:
        st.info("Nenhum ajuste de saldo negativo foi necessário no período para esta conta contábil.")
        return pd.DataFrame()

    return pd.DataFrame(lancamentos_propostos)


def gerar_lancamentos_saldo_negativo_contabil_cadastro(
    conta_selecionada_row: pd.Series,
    data_inicio: datetime.date,
    data_fim: datetime.date
) -> pd.DataFrame:
    """
    Analisa o saldo diário de uma conta contábil de banco (baseado nos lançamentos contábeis)
    e gera lançamentos de ajuste para cobrir saldos negativos (credores).

    Usa o cadastro de contas bancárias para obter as contas contábeis.
    Similar a gerar_lancamentos_saldo_negativo, mas usa lançamentos contábeis em vez de extratos OFX.
    """
    # 1. Validar informações da conta do cadastro
    conta_contabil_principal = conta_selecionada_row.get('Conta Contábil')
    conta_contabil_negativo = conta_selecionada_row.get('Conta Contábil (-)')

    if not conta_contabil_principal or pd.isna(conta_contabil_principal) or not conta_contabil_negativo or pd.isna(conta_contabil_negativo):
        st.error("A conta bancária selecionada não possui a 'Conta Contábil' e/ou a 'Conta Contábil (-)' preenchidas no cadastro. Verifique o Menu 1.1.")
        return pd.DataFrame()

    # Converter para string
    conta_contabil_principal = str(int(conta_contabil_principal))
    conta_contabil_negativo = str(int(conta_contabil_negativo))

    plano_contas = carregar_plano_contas()
    mapa_nomes_contas = plano_contas.set_index('codigo')['descricao'].to_dict()

    nome_conta_principal = mapa_nomes_contas.get(conta_contabil_principal, "NOME NÃO ENCONTRADO")
    nome_conta_negativo = mapa_nomes_contas.get(conta_contabil_negativo, "NOME NÃO ENCONTRADO")

    # 2. Obter saldo inicial do cadastro
    saldo_inicial_cadastro = conta_selecionada_row.get('Saldo Inicial', 0.0)
    if pd.isna(saldo_inicial_cadastro):
        saldo_inicial_cadastro = 0.0

    data_saldo_inicial_str = conta_selecionada_row.get('Data Inicial Saldo')
    data_saldo_inicial = datetime.datetime.strptime(data_saldo_inicial_str, '%d%m%Y').date() if data_saldo_inicial_str and pd.notna(data_saldo_inicial_str) else None

    # 3. Carregar lançamentos contábeis
    df_lancamentos = carregar_lancamentos_contabeis()

    if df_lancamentos.empty:
        st.warning("Não há lançamentos contábeis cadastrados.")
        return pd.DataFrame()

    # Converter data_lancamento para datetime
    df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'], errors='coerce')

    # Filtrar lançamentos que envolvem a conta principal (até data_fim)
    mask_debito = df_lancamentos['reduz_deb'] == conta_contabil_principal
    mask_credito = df_lancamentos['reduz_cred'] == conta_contabil_principal
    mask_data = df_lancamentos['data_lancamento'].dt.date <= data_fim

    # Define o início da busca de transações
    data_inicio_busca = data_saldo_inicial if data_saldo_inicial else datetime.date(2000, 1, 1)
    mask_data = mask_data & (df_lancamentos['data_lancamento'].dt.date >= data_inicio_busca)

    df_conta = df_lancamentos[(mask_debito | mask_credito) & mask_data].copy()

    # 4. Calcular movimento diário
    # Para contas de Ativo (banco): Débito aumenta (+), Crédito diminui (-)
    def calcular_movimento(row):
        if row['reduz_deb'] == conta_contabil_principal:
            return row['valor']  # Débito aumenta saldo
        else:
            return -row['valor']  # Crédito diminui saldo

    if not df_conta.empty:
        df_conta['movimento'] = df_conta.apply(calcular_movimento, axis=1)
        movimento_diario = df_conta.groupby(df_conta['data_lancamento'].dt.date)['movimento'].sum()
    else:
        movimento_diario = pd.Series(dtype=float)

    # 5. Criar range de datas completo
    start_date_calc = data_saldo_inicial if data_saldo_inicial else (min(movimento_diario.index) if not movimento_diario.empty else data_inicio)

    all_dates = pd.date_range(start=start_date_calc, end=data_fim, freq='D')

    df_saldos = pd.DataFrame(index=all_dates)
    df_saldos['movimento'] = movimento_diario
    df_saldos['movimento'].fillna(0, inplace=True)

    # Calcular saldo acumulado
    df_saldos['saldo_final'] = saldo_inicial_cadastro + df_saldos['movimento'].cumsum()

    # 6. Gerar lançamentos de ajuste
    lancamentos_propostos = []
    saldo_provisionado_anterior = 0

    # Calcular provisão existente antes do período de análise
    df_antes_periodo = df_saldos[df_saldos.index.date < data_inicio]
    if not df_antes_periodo.empty:
        saldo_final_anterior = df_antes_periodo.iloc[-1]['saldo_final']
        if saldo_final_anterior < 0:
            saldo_provisionado_anterior = abs(saldo_final_anterior)

    # Iterar sobre dias no período selecionado
    df_periodo_analise = df_saldos[(df_saldos.index.date >= data_inicio) & (df_saldos.index.date <= data_fim)]

    for data, row in df_periodo_analise.iterrows():
        saldo_do_dia = row['saldo_final']
        valor_a_provisionar_hoje = abs(saldo_do_dia) if saldo_do_dia < 0 else 0

        ajuste_necessario = valor_a_provisionar_hoje - saldo_provisionado_anterior

        if round(ajuste_necessario, 2) != 0:
            idlanc = str(uuid.uuid4())
            data_lanc_str = data.strftime('%Y-%m-%d')
            valor_ajuste = abs(ajuste_necessario)

            if ajuste_necessario > 0:  # Provisão ou aumento da provisão (saldo ficou mais negativo)
                lanc = {
                    'idlancamento': idlanc, 'data_lancamento': data_lanc_str,
                    'historico': f"Provisão para cobertura de saldo credor contábil em {data.strftime('%d/%m/%Y')}",
                    'valor': valor_ajuste, 'tipo_lancamento': 'Ajuste',
                    'reduz_deb': conta_contabil_principal, 'nome_conta_d': nome_conta_principal,
                    'reduz_cred': conta_contabil_negativo, 'nome_conta_c': nome_conta_negativo,
                    'origem': 'conta contabil negativa'
                }
                lancamentos_propostos.append(lanc)
            else:  # Reversão ou diminuição da provisão (saldo ficou menos negativo)
                lanc = {
                    'idlancamento': idlanc, 'data_lancamento': data_lanc_str,
                    'historico': f"Reversão de provisão de saldo credor contábil em {data.strftime('%d/%m/%Y')}",
                    'valor': valor_ajuste, 'tipo_lancamento': 'Ajuste',
                    'reduz_deb': conta_contabil_negativo, 'nome_conta_d': nome_conta_negativo,
                    'reduz_cred': conta_contabil_principal, 'nome_conta_c': nome_conta_principal,
                    'origem': 'conta contabil negativa'
                }
                lancamentos_propostos.append(lanc)

        saldo_provisionado_anterior = valor_a_provisionar_hoje

    if not lancamentos_propostos:
        st.info("Nenhum ajuste de saldo negativo foi necessário no período para esta conta contábil.")
        return pd.DataFrame()

    return pd.DataFrame(lancamentos_propostos)
