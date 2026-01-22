# data_loader.py - VERSÃO FINAL E COMPLETA (SEM DEBUG)
import pandas as pd
import streamlit as st
from ofxparse import OfxParser
from io import BytesIO, StringIO
from datetime import date
from typing import Tuple
import re
import numpy as np
import os
import unicodedata
import pdfplumber
import hashlib

# Importação Absoluta
# É CRUCIAL que o utils.py esteja na versão mais recente
from utils import normalizar_numero, safe_parse_date, extrair_conta_ofx_bruta, normalizar_chave_ofx


# ==============================================================================
# 1. FUNÇÕES DE CARREGAMENTO DO OFX (Extrato Bancário)
# ==============================================================================

# CACHE TEMPORARIAMENTE DESABILITADO PARA FORÇAR REPROCESSAMENTO
# @st.cache_data(show_spinner="Processando arquivo OFX...")
def importar_extrato_ofx(file_bytes, file_name, df_cadastro=None):
    """Lê um arquivo OFX, extrai transações e retorna um DataFrame padronizado.

    Args:
        file_bytes: Bytes do arquivo OFX
        file_name: Nome do arquivo
        df_cadastro: DataFrame do cadastro de contas (opcional, para corrigir dados incompletos)
    """
    st.info("Processando arquivo OFX...")
    try:
        # Tentar diferentes encodings para lidar com arquivos do Brasil
        encodings_to_try = ['cp1252', 'latin-1', 'iso-8859-1', 'utf-8']
        ofx = None
        last_error = None
        file_content = None

        for encoding in encodings_to_try:
            try:
                # Decodificar com o encoding específico
                file_content = file_bytes.decode(encoding)

                # Remover acentos para garantir compatibilidade com a biblioteca ofxparse
                # A biblioteca ofxparse força ASCII internamente, ignorando o cabeçalho
                def remove_accents(text):
                    nfd = unicodedata.normalize('NFD', text)
                    return ''.join(char for char in nfd if unicodedata.category(char) != 'Mn')

                file_content = remove_accents(file_content)

                # Tentar fazer o parsing usando StringIO (texto puro)
                ofx = OfxParser.parse(StringIO(file_content))
                break
            except (UnicodeDecodeError, UnicodeError) as e:
                last_error = e
                continue
            except Exception as e:
                # Se não for erro de encoding, provavelmente é outro problema
                last_error = e
                continue

        if ofx is None:
            raise Exception(f"Não foi possível decodificar o arquivo OFX. Último erro: {last_error}")

        transactions = []

        # Tenta extrair a conta bruta do arquivo como fallback de primeira (GARANTIA SICREDI)
        conta_raw_bruta = extrair_conta_ofx_bruta(file_bytes)

        for account in ofx.accounts:

            # 1. Extração da Conta e Banco (Robusta e com Fallback Bruto)
            banco_identificador = 'Desconhecido'
            conta_raw = ''
            agencia_raw = ''

            # Tenta encontrar a estrutura de dados de conta
            account_data = None
            if hasattr(account, 'bank_account'):
                account_data = account.bank_account
            elif hasattr(account, 'statement_account'):
                account_data = account.statement_account
            elif hasattr(account, 'statement') and hasattr(account.statement, 'statement_account'):
                account_data = account.statement.statement_account

            # --- Extração da Conta ---
            # Tenta primeiro os atributos diretos do account (ofxparse padrão)
            conta_raw = getattr(account, 'account_id', '') or getattr(account, 'number', '')
            banco_identificador = getattr(account, 'routing_number', 'Desconhecido')
            agencia_raw = getattr(account, 'branch_id', '')

            # Fallback para account_data (se existir)
            if account_data and not conta_raw:
                conta_raw = getattr(account_data, 'number', '') or getattr(account_data, 'id', '')
            if account_data and banco_identificador == 'Desconhecido':
                banco_identificador = getattr(account_data, 'bankid', 'Desconhecido')
            if account_data and not agencia_raw:
                agencia_raw = getattr(account_data, 'branchid', '')

            # Prioridade para o nome do banco se for uma instituição conhecida (ex: Sicredi)
            if hasattr(account, 'institution') and hasattr(account.institution, 'org'):
                org_name = account.institution.org.lower()
                if 'sicredi' in org_name:
                    banco_identificador = '748'

            # Fallback final para BANKID se ainda for desconhecido, usando regex
            if banco_identificador in ['Desconhecido', '000', 'Desconhecido_2']:
                # Usar file_content que já foi decodificado corretamente acima
                bankid_match = re.search(r'<BANKID>(\d+)', file_content)
                if bankid_match:
                    banco_identificador = bankid_match.group(1)

            # 1b. Fallback 1: Tenta extrair da propriedade ID da conta (ACCTID)
            if not conta_raw:
                conta_raw = getattr(account, 'id', '')

            # 1c. Fallback 2: Usa a extração bruta do arquivo (DEVE FUNCIONAR PARA O SICREDI)
            if not conta_raw:
                conta_raw = conta_raw_bruta

            # 1d. Fallback de Banco (Instituição)
            if banco_identificador == 'Desconhecido' and hasattr(account, 'institution'):
                banco_identificador = getattr(account.institution, 'fid', 'Desconhecido_2')

            # --- Limpeza e Normalização Final ---
            # 1. Limpa o código do banco para ser apenas numérico
            banco_match = re.search(r'\d+', str(banco_identificador))
            if banco_match:
                banco_identificador = banco_match.group(0)

            # Regra especial para Daycoval (707)
            if banco_identificador == '707':
                agencia_raw = '0001'

            # Regra especial para Banco do Brasil (001) - Extrai agência e conta do nome do arquivo
            if banco_identificador == '001':
                # Tenta extrair do padrão "Extrato<AGENCIA><CONTA>.ofx"
                # Exemplo: Extrato152371347578.ofx -> agencia=1523, conta=71347578 ou 1347578
                match_bb = re.search(r'[Ee]xtrato(\d{4})(\d+)', file_name)
                if match_bb:
                    agencia_raw = match_bb.group(1)  # Primeiros 4 dígitos
                    conta_extraida = match_bb.group(2)  # Resto dos dígitos

                    # Remove possível dígito inicial extra (ex: 71347578 -> 1347578)
                    # Verifica se o primeiro dígito da conta_extraida pode ser removido
                    # comparando com o ACCTID do OFX
                    conta_ofx_limpa = re.sub(r'\D', '', str(conta_raw))  # Remove não-dígitos do ACCTID

                    # Tenta encontrar correspondência
                    if len(conta_extraida) > len(conta_ofx_limpa) and conta_extraida[1:] == conta_ofx_limpa:
                        # Caso: 71347578 do arquivo vs 1347578 do OFX
                        conta_raw = conta_extraida[1:]  # Remove primeiro dígito
                    elif len(conta_extraida) >= len(conta_ofx_limpa) and conta_extraida[-len(conta_ofx_limpa):] == conta_ofx_limpa:
                        # Caso: a conta do arquivo contém a conta do OFX no final
                        conta_raw = conta_extraida
                    else:
                        # Usa a conta extraída do arquivo como está
                        conta_raw = conta_extraida

            # 2. Limpa o número da conta para extrair apenas a parte numérica relevante
            numeros_encontrados = re.findall(r'\d+', str(conta_raw))
            if numeros_encontrados:
                conta_raw = max(numeros_encontrados, key=len)

            # Regra especial para Bradesco (237) - O Bradesco exporta OFX com dados incompletos
            # ACCTID vem truncado (ex: "10" ao invés de "108") e não inclui BRANCHID
            if banco_identificador == '237':
                # Tentar extrair BRANCHID (agência) do conteúdo - geralmente não existe
                branchid_match = re.search(r'<BRANCHID>(\d+)', file_content)
                if branchid_match:
                    agencia_raw = branchid_match.group(1)

                # Tentar extrair ACCTID (conta) do conteúdo
                acctid_match = re.search(r'<ACCTID>([^<\n\r]+)', file_content)
                if acctid_match:
                    acctid_full = acctid_match.group(1).strip()
                    # Remover hífen e dígito verificador (ex: "108-1" -> "108")
                    conta_sem_digito = acctid_full.split('-')[0] if '-' in acctid_full else acctid_full
                    # Extrair apenas números
                    numeros = re.findall(r'\d+', conta_sem_digito)
                    if numeros:
                        conta_raw = max(numeros, key=len)

                # IMPORTANTE: O OFX do Bradesco vem incompleto
                # Tentar buscar no cadastro de contas automaticamente
                bradesco_incompleto = False

                # DEBUG: Mostrar valores extraídos
                st.info(f"🔍 DEBUG Bradesco - Valores extraídos do OFX:")
                st.info(f"   - agencia_raw: '{agencia_raw}' (tipo: {type(agencia_raw).__name__})")
                st.info(f"   - conta_raw: '{conta_raw}' (tipo: {type(conta_raw).__name__}, tamanho: {len(str(conta_raw))})")
                st.info(f"   - Cadastro recebido: {df_cadastro is not None and not df_cadastro.empty if df_cadastro is not None else False}")

                if not agencia_raw or len(str(conta_raw)) < 3:
                    bradesco_incompleto = True
                    st.warning(f"⚠️ Detectado: Bradesco com dados incompletos")

                    # Tentar encontrar no cadastro de contas
                    conta_encontrada = False
                    if df_cadastro is not None and not df_cadastro.empty:
                        st.info(f"📋 Cadastro disponível com {len(df_cadastro)} linhas")

                        # DEBUG: Mostrar códigos de banco disponíveis
                        if 'Codigo_Banco' in df_cadastro.columns:
                            codigos_unicos = df_cadastro['Codigo_Banco'].unique()
                            st.info(f"   - Códigos de banco no cadastro: {list(codigos_unicos)}")

                        # Filtrar contas do Bradesco no cadastro (aceita '237' ou '0237')
                        contas_bradesco = df_cadastro[df_cadastro['Codigo_Banco'].isin(['237', '0237'])]
                        st.info(f"   - Contas do Bradesco (237/0237) encontradas: {len(contas_bradesco)}")

                        if len(contas_bradesco) == 1:
                            # Se houver apenas uma conta do Bradesco, usar essa
                            agencia_raw = contas_bradesco.iloc[0]['Agencia']
                            conta_cadastro = contas_bradesco.iloc[0]['Conta']
                            # Remover dígito verificador se houver
                            conta_raw = conta_cadastro.split('-')[0] if '-' in str(conta_cadastro) else conta_cadastro
                            conta_encontrada = True
                            st.success(f"✅ Dados do Bradesco corrigidos automaticamente usando o Cadastro de Contas:")
                            st.success(f"   - Agência: {agencia_raw} | Conta: {conta_raw}")
                        elif len(contas_bradesco) > 1:
                            st.warning(f"⚠️ Encontradas {len(contas_bradesco)} contas do Bradesco no cadastro. Não foi possível determinar qual usar automaticamente.")
                    else:
                        st.error(f"❌ Cadastro de contas NÃO disponível (df_cadastro={'None' if df_cadastro is None else 'vazio'})")

                    if not conta_encontrada:
                        # Usar o ACCTID original como identificador temporário
                        conta_raw = f"BRADESCO_OFX_{acctid_full}" if acctid_match else "BRADESCO_OFX_DESCONHECIDO"
                        st.error(f"⚠️ ATENÇÃO: Arquivo {file_name} do Bradesco contém dados INCOMPLETOS no OFX!")
                        st.error(f"   - Agência no OFX: {agencia_raw or 'NÃO INFORMADA'}")
                        st.error(f"   - Conta no OFX: {acctid_full if acctid_match else 'NÃO INFORMADA'}")
                        st.info(f"   💡 IMPORTANTE: O Bradesco exporta OFX com dados truncados.")
                        st.warning(f"   📋 RECOMENDAÇÃO: Use os arquivos CSV do Bradesco ao invés de OFX, pois os CSVs contêm agência e conta completas.")
                        st.warning(f"   📋 SOLUÇÃO: Importe o Cadastro de Contas com uma única conta do Bradesco (Código 237, Agência 2115, Conta 108) e reimporte este arquivo.")

            # 3. Normalização da Chave OFX (Agência + Conta)
            agencia_normalizada = normalizar_numero(agencia_raw).zfill(4) if agencia_raw else ''

            # Evita duplicar a agência se já estiver na conta (comum no Sicredi)
            if banco_identificador == '748' and agencia_normalizada and conta_raw.startswith(agencia_normalizada):
                 chave_bruta = conta_raw
            else:
                 chave_bruta = agencia_normalizada + conta_raw

            conta_normalizada = normalizar_chave_ofx(chave_bruta)

            # Garante a string do Banco
            banco_identificador = str(banco_identificador).strip()
            if not banco_identificador or not banco_identificador.isdigit():
                banco_identificador = '000' # Usa um código padrão se tudo falhar

            # --- 2. Busca pela Lista de Transações (ROBUSTA) ---
            transaction_list = []
            if account_data and hasattr(account_data, 'transactions'):
                transaction_list = account_data.transactions
            if not transaction_list and hasattr(account, 'statement') and hasattr(account.statement, 'transactions'):
                transaction_list = account.statement.transactions
            if not transaction_list and hasattr(account, 'transactions'):
                transaction_list = account.transactions

            if not transaction_list:
                st.error(
                    f"ERRO: Nenhuma transação encontrada para a conta '{conta_normalizada}' no arquivo {file_name}. O OFX não pôde ser lido corretamente.")
                continue

            # 3. Processamento das transações
            total_original = len(transaction_list)
            total_saldo_dia = 0
            total_processadas = 0

            # Detectar período principal do extrato e filtrar transações
            from datetime import datetime, timedelta
            hoje = date.today()

            # Analisar todas as datas para identificar o período principal do extrato
            periodo_mes = None
            periodo_ano = None

            if len(transaction_list) > 0:
                # Contar transações por mês/ano
                periodos = {}
                for t in transaction_list:
                    dt = safe_parse_date(t.date, hoje)
                    periodo_key = (dt.year, dt.month)
                    periodos[periodo_key] = periodos.get(periodo_key, 0) + 1

                # Identificar o período com mais transações (período principal do extrato)
                if periodos:
                    periodo_principal = max(periodos, key=periodos.get)
                    periodo_ano, periodo_mes = periodo_principal
                    total_periodo_principal = periodos[periodo_principal]
                    total_outros_periodos = sum(v for k, v in periodos.items() if k != periodo_principal)

                    # Se há transações de outros períodos, avisar o usuário
                    if len(periodos) > 1 and total_outros_periodos > 0:
                        outros_periodos_str = ', '.join([f"{m:02d}/{a}" for (a, m), count in periodos.items() if (a, m) != periodo_principal])
                        st.warning(f"⚠️ Arquivo {file_name} contém transações de múltiplos períodos. "
                                  f"Importando APENAS {total_periodo_principal} transações de {periodo_mes:02d}/{periodo_ano}. "
                                  f"Ignorando {total_outros_periodos} transações de outros períodos: {outros_periodos_str}")
                    else:
                        st.success(f"✓ Importando {total_periodo_principal} transações do período {periodo_mes:02d}/{periodo_ano}")

            transacoes_filtradas = 0
            for t in transaction_list:
                # Ignora transações de "SALDO DO DIA" da Caixa (são apenas marcadores)
                descricao = t.payee or t.memo or ''
                if 'SALDO DO DIA' in descricao.upper():
                    total_saldo_dia += 1
                    continue

                data_lancamento = safe_parse_date(t.date, date.today())
                data_processamento = safe_parse_date(getattr(t, 'date_user', t.date), data_lancamento)

                # Filtrar transações fora do período principal
                if periodo_mes and periodo_ano:
                    if data_lancamento.year != periodo_ano or data_lancamento.month != periodo_mes:
                        transacoes_filtradas += 1
                        continue

                transactions.append({
                    'Data Lançamento': data_lancamento,
                    'Data Processamento': data_processamento,
                    'Valor': t.amount,
                    'Descrição': descricao,
                    'ID Transacao': t.id,
                    'Tipo': t.type,
                    'Banco_OFX': banco_identificador,
                    'Conta_OFX_Normalizada': conta_normalizada
                })
                total_processadas += 1

            # Mostra informações de processamento se houver filtros aplicados
            # if total_saldo_dia > 0:
            #     st.info(f"Arquivo: {file_name} | {total_processadas} transações válidas ({total_saldo_dia} marcadores 'SALDO DO DIA' filtrados)")

        if not transactions:
            st.error(f"Arquivo {file_name}: Nenhuma transação foi processada.")
            return pd.DataFrame()

        df = pd.DataFrame(transactions)
        df['Entrada'] = df['Valor'].apply(lambda x: x if x > 0 else 0)
        df['Saída'] = df['Valor'].apply(lambda x: abs(x) if x < 0 else 0)

        return df

    except Exception as e:
        st.error(f"Erro crítico ao processar o arquivo OFX ({file_name}): {e}")
        return pd.DataFrame()


def importar_extrato_excel_daycoval(file_bytes, file_name):
    """Importa extratos do Banco Daycoval em formato Excel (.xls ou .xlsx)."""
    try:
        # Ler o arquivo Excel
        df = pd.read_excel(BytesIO(file_bytes), header=None)

        # Verificar se é um arquivo do Daycoval (primeira linha deve conter 'agencia' e 'conta')
        primeira_linha = df.iloc[0].astype(str).str.lower().str.strip()
        if 'agencia' not in primeira_linha.values or 'conta' not in primeira_linha.values:
            st.warning(f"O arquivo '{file_name}' não parece ser um extrato do Daycoval.")
            return pd.DataFrame()

        # Extrair conta e agência do cabeçalho (linha 0)
        # Formato esperado: ['agencia', '1', 'conta', '611375-5', ...]
        # Agência está na coluna B (índice 1), Conta está na coluna D (índice 3)
        conta_raw = str(df.iloc[0, 3]) if len(df.columns) > 3 else 'Desconhecida'
        agencia_raw = str(df.iloc[0, 1]) if len(df.columns) > 1 else '0001'

        # Normalizar agência (preencher com zeros à esquerda para 4 dígitos)
        agencia_raw = normalizar_numero(agencia_raw).zfill(4)

        # Normalizar conta (remover hífen e dígito verificador)
        conta_sem_digito = conta_raw.split('-')[0] if '-' in conta_raw else conta_raw
        conta_raw = normalizar_numero(conta_sem_digito)

        # Remover a linha de cabeçalho
        df = df.iloc[1:].reset_index(drop=True)

        # Remover linhas vazias
        df = df.dropna(subset=[0, 1, 2], how='all')  # Remove linhas completamente vazias

        # Remover última linha APENAS se for totalizador (linha vazia ou com texto "Total")
        if len(df) > 0:
            ultima_linha = df.iloc[-1]
            # Verifica se a última linha é totalizador (sem data válida ou com texto "Total")
            if pd.isna(ultima_linha[0]) or 'total' in str(ultima_linha[1]).lower() or 'total' in str(ultima_linha[2]).lower():
                df = df[:-1]

        # Renomear colunas
        # Coluna 0: Data, 1: Documento, 2: Histórico, 3: Débito, 4: Crédito, 5: Saldo
        df.columns = ['Data Lançamento', 'Documento', 'Memo', 'Debito', 'Credito', 'Saldo']

        # Converter data
        df['Data Lançamento'] = pd.to_datetime(df['Data Lançamento'], errors='coerce')

        # Remover linhas com data inválida
        df = df.dropna(subset=['Data Lançamento'])

        # Processar valores: Débito negativo, Crédito positivo
        df['Debito'] = pd.to_numeric(df['Debito'], errors='coerce').fillna(0)
        df['Credito'] = pd.to_numeric(df['Credito'], errors='coerce').fillna(0)

        # Calcular valor final (Crédito - Débito)
        df['Valor'] = df['Credito'] - df['Debito']

        # Normalizar histórico (usar como Descrição)
        df['Descrição'] = df['Memo'].astype(str).str.strip()

        # Adicionar informações do banco
        df['Banco_OFX'] = '707'  # Código do Daycoval

        # Criar a chave normalizada (agência já está com 4 dígitos e conta sem dígito)
        chave_bruta = agencia_raw + conta_raw
        df['Conta_OFX_Normalizada'] = normalizar_chave_ofx(chave_bruta)

        # Gerar ID único para cada transação (hash de data + descrição + valor + conta + índice)
        import hashlib
        def gerar_id_transacao(row):
            id_str = f"{row['Data Lançamento']}{row['Descrição']}{row['Valor']}{chave_bruta}{row.name}"
            return hashlib.md5(id_str.encode()).hexdigest()

        df['ID Transacao'] = df.apply(gerar_id_transacao, axis=1)
        df['Tipo'] = df['Valor'].apply(lambda x: 'CREDIT' if x > 0 else 'DEBIT')
        df['Data Processamento'] = df['Data Lançamento']

        # Adicionar colunas de Entrada e Saída
        df['Entrada'] = df['Valor'].apply(lambda x: x if x > 0 else 0)
        df['Saída'] = df['Valor'].apply(lambda x: abs(x) if x < 0 else 0)

        # Selecionar e ordenar colunas no padrão do sistema (igual ao OFX)
        df = df[['Data Lançamento', 'Data Processamento', 'Valor', 'Descrição',
                 'ID Transacao', 'Tipo', 'Banco_OFX', 'Conta_OFX_Normalizada',
                 'Entrada', 'Saída']]

        st.success(f"Arquivo Excel Daycoval '{file_name}' importado: {len(df)} transações")
        return df

    except Exception as e:
        st.error(f"Erro ao processar arquivo Excel Daycoval '{file_name}': {e}")
        st.exception(e)
        return pd.DataFrame()


# CACHE TEMPORARIAMENTE DESABILITADO
# @st.cache_data
def importar_multiplos_extratos(uploaded_files, df_cadastro=None):
    """Importa múltiplos arquivos de extrato (OFX, PDF, Excel) e concatena em um único DataFrame.

    Args:
        uploaded_files: Lista de arquivos enviados
        df_cadastro: DataFrame do cadastro de contas (opcional, para corrigir dados incompletos do Bradesco)
    """
    all_dfs = []
    for file in uploaded_files:
        file_bytes = file.getvalue()
        file_name = file.name
        df = pd.DataFrame()

        if file_name.lower().endswith(('.ofx', '.ofc')):
            df = importar_extrato_ofx(file_bytes, file_name, df_cadastro=df_cadastro)
        elif file_name.lower().endswith(('.xls', '.xlsx')):
            df = importar_extrato_excel_daycoval(file_bytes, file_name)
        elif file_name.lower().endswith('.pdf'):
            try:
                with pdfplumber.open(BytesIO(file_bytes)) as pdf:
                    if not pdf.pages:
                        st.error(f"O arquivo PDF '{file_name}' não tem páginas.")
                        continue

                    first_page_text = pdf.pages[0].extract_text()

                    if 'sicredi' in first_page_text.lower():
                        st.success("PDF do Sicredi detectado.")
                        df = importar_extrato_pdf_sicredi(file_bytes, file_name)
                    elif 'santander' in first_page_text.lower() or 'extrato consolidado' in first_page_text.lower():
                        st.success("PDF do Santander detectado.")
                        df = importar_extrato_pdf_santander(file_bytes, file_name)
                    else:
                        st.warning(f"O arquivo PDF '{file_name}' não é de um banco suportado e foi ignorado.")
            except Exception as e:
                st.warning(f"Não foi possível ler o PDF '{file_name}'. Erro: {e}")

        if not df.empty:
            all_dfs.append(df)

    if all_dfs:
        df_final = pd.concat(all_dfs, ignore_index=True)

        if 'Banco_OFX' in df_final.columns:
            df_final['Banco_OFX'] = df_final['Banco_OFX'].astype(str).replace(['None', 'none', 'nan'], 'Desconhecido')

        if 'Conta_OFX_Normalizada' in df_final.columns:
            df_final['Conta_OFX_Normalizada'] = df_final['Conta_OFX_Normalizada'].apply(normalizar_chave_ofx)

        return df_final
    return pd.DataFrame()


# ==============================================================================
# 2. FUNÇÃO DE CARREGAMENTO DO EXTRATO CONTÁBIL
# ==============================================================================

def tratar_lancamentos_problematicos(df):
    """
    Aplica regras de tratamento para lançamentos com débito = crédito na mesma conta.

    Regras:
    1. Conta 8 em débito e crédito: débito 302 (CAIXA GERAL), crédito 8
    2. Conta 395 em débito e crédito (sem 'estorno'): débito 395, crédito 70 (AJUSTE CONTABIL)
    3. Conta 395 em débito e crédito + 'estorno' no histórico: EXCLUIR lançamento
    """
    if df.empty:
        return df

    # Função auxiliar para normalizar conta
    def normalizar_conta(valor):
        try:
            return int(float(valor))
        except (ValueError, TypeError):
            return None

    # Função auxiliar para verificar se o histórico contém 'estorno'
    def contem_estorno(historico):
        if not isinstance(historico, str):
            return False
        return 'estorno' in historico.lower()

    # Identificar lançamentos por ID para processar em grupo
    if 'ID Lancamento' not in df.columns:
        return df

    ids_para_excluir = []
    registros_modificados = 0

    for id_lanc in df['ID Lancamento'].dropna().unique():
        grupo_idx = df[df['ID Lancamento'] == id_lanc].index
        grupo = df.loc[grupo_idx]

        # Pegar contas de débito e crédito
        reduz_deb_vals = grupo['ReduzDeb'].dropna().unique()
        reduz_cred_vals = grupo['ReduzCred'].dropna().unique()

        # Processar apenas se tem 1 débito e 1 crédito
        if len(reduz_deb_vals) == 1 and len(reduz_cred_vals) == 1:
            conta_deb = normalizar_conta(reduz_deb_vals[0])
            conta_cred = normalizar_conta(reduz_cred_vals[0])

            # Só processar se débito = crédito
            if conta_deb is not None and conta_cred is not None and conta_deb == conta_cred:
                historico = grupo['Historico'].iloc[0] if 'Historico' in grupo.columns else ''

                # REGRA 3: Conta 395 + 'estorno' no histórico = EXCLUIR
                if conta_deb == 395 and contem_estorno(historico):
                    ids_para_excluir.append(id_lanc)
                    continue

                # REGRA 1: Conta 8 em débito e crédito -> débito 302, crédito 8
                elif conta_deb == 8:
                    # Alterar linha de débito para conta 302
                    for idx in grupo_idx:
                        if pd.notna(df.loc[idx, 'ReduzDeb']):
                            df.loc[idx, 'ReduzDeb'] = 302
                            df.loc[idx, 'NomeContaD'] = 'CAIXA GERAL'
                    registros_modificados += 1

                # REGRA 2: Conta 395 em débito e crédito (sem estorno) -> débito 395, crédito 70
                elif conta_deb == 395:
                    # Alterar linha de crédito para conta 70
                    for idx in grupo_idx:
                        if pd.notna(df.loc[idx, 'ReduzCred']):
                            df.loc[idx, 'ReduzCred'] = 70
                            df.loc[idx, 'NomeContaC'] = 'AJUSTE CONTABIL'
                    registros_modificados += 1

    # Excluir lançamentos marcados
    if ids_para_excluir:
        df = df[~df['ID Lancamento'].isin(ids_para_excluir)]
        st.info(f"🗑️ {len(ids_para_excluir)} lançamentos de estorno (conta 395 deb/cred) foram excluídos automaticamente.")

    if registros_modificados > 0:
        st.info(f"✏️ {registros_modificados} lançamentos com débito=crédito na mesma conta foram ajustados automaticamente.")

    return df


@st.cache_data(show_spinner="Lendo Extrato Contábil...")
def ler_extrato_contabil(uploaded_file):
    """Lê o arquivo Excel/CSV do extrato contábil e retorna um DataFrame padronizado."""
    file_extension = uploaded_file.name.split('.')[-1].lower()

    try:
        if file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(uploaded_file, engine='openpyxl')
        elif file_extension == 'csv':
            uploaded_file.seek(0)
            try:
                df = pd.read_csv(uploaded_file, sep=';', encoding='latin1', on_bad_lines='skip')
            except Exception:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, sep=',', encoding='utf-8', on_bad_lines='skip')
        else:
            st.error("Formato de arquivo contábil não suportado. Use .xlsx, .xls ou .csv.")
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Erro na leitura do arquivo contábil: {e}")
        return pd.DataFrame()

    df.columns = [str(col).strip().title().replace(' ', '') for col in df.columns]

    # Mapeamento e criação das colunas necessárias
    df.rename(columns={
        'Data': 'Data Lançamento',
        'Idlancamento': 'ID Lancamento',
        'Descricao': 'Historico',
        'Reduzdeb': 'ReduzDeb',
        'Nomecontad': 'NomeContaD',
        'Reduzcred': 'ReduzCred',
        'Nomecontac': 'NomeContaC'
    }, inplace=True)

    colunas_necessarias = ['Data Lançamento', 'Valor', 'Historico', 'ReduzDeb', 'NomeContaD', 'ReduzCred', 'NomeContaC']
    if not all(col in df.columns for col in colunas_necessarias):
        st.error(f"Colunas obrigatórias não encontradas no arquivo contábil. Esperadas: {colunas_necessarias}")
        st.info(f"Colunas encontradas após o processamento: {list(df.columns)}")
        return pd.DataFrame()

    if 'Historico' in df.columns:
        def strip_accents(text):
            if not isinstance(text, str):
                return ""
            return ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')

        historico_normalized = df['Historico'].astype(str).str.strip().str.lower().apply(strip_accents)

        cond1 = historico_normalized.str.startswith('lancamento')
        cond2 = historico_normalized.str.startswith('estorno da contabilizacao do lancamento')

        df['Tipo Lancamento'] = np.where(cond1 | cond2, 'Inclusão', 'Baixa')

    df['Data Lançamento'] = df['Data Lançamento'].apply(lambda x: safe_parse_date(x, date.today()))

    if df['Valor'].dtype == 'object':
        df['Valor'] = df['Valor'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    df['Valor'] = pd.to_numeric(df['Valor'], errors='coerce')

    df.dropna(subset=['Data Lançamento', 'Valor'], inplace=True)

    # Adiciona a coluna de origem
    df['Origem'] = 'Sistema Origem'

    # =====================================================
    # TRATAMENTO DE LANÇAMENTOS PROBLEMÁTICOS
    # Aplica regras para corrigir débito = crédito na mesma conta
    # =====================================================
    df = tratar_lancamentos_problematicos(df)

    # Gera um ID único para o lote de importação (não usado como chave primária)
    # df['ID Lancamento'] = range(1, len(df) + 1)

    return df


# ==============================================================================
# 3. FUNÇÃO DE CARREGAMENTO DO CADASTRO DE CONTAS (FINALIZADO COM MAPA DE ÍNDICE E SEM CABEÇALHO)
# ==============================================================================

@st.cache_data(show_spinner="Lendo Cadastro de Contas...")
def ler_cadastro_contas(uploaded_file):
    """Lê o arquivo de cadastro de contas bancárias (CSV ou Excel) e retorna um DataFrame."""
    file_extension = uploaded_file.name.split('.')[-1].lower()
    df = pd.DataFrame()

    try:
        dtype_str = str 
        if file_extension in ['xlsx', 'xls']:
            df = pd.read_excel(uploaded_file, engine='openpyxl', header=None, dtype=dtype_str)
        elif file_extension == 'csv':
            uploaded_file.seek(0)
            try:
                df = pd.read_csv(uploaded_file, sep=';', encoding='latin1', header=None, dtype=dtype_str)
            except Exception:
                uploaded_file.seek(0)
                df = pd.read_csv(uploaded_file, sep=',', encoding='latin1', header=None, dtype=dtype_str)

            if df.empty:
                st.error("Falha na leitura do CSV: O arquivo está vazio ou o formato é irreconhecível.")
                return pd.DataFrame()
        else:
            st.error("Formato de arquivo não suportado para Cadastro de Contas.")
            return pd.DataFrame()

    except Exception as e:
        st.error(f"Erro inesperado na leitura do cadastro de contas: {e}")
        return pd.DataFrame()

    df.columns = [str(col).strip() for col in df.columns]

    col_mapping_indices = {
        '0': 'Codigo_Banco',
        '1': 'Agencia',
        '2': 'Conta',
        '3': 'Data Inicial Saldo',
        '4': 'Conta Contábil',
        '5': 'Saldo Inicial',
        '6': 'Conta Contábil (-)'
    }
    df.rename(columns={k: v for k, v in col_mapping_indices.items() if k in df.columns}, inplace=True)

    colunas_obrigatorias = ['Agencia', 'Conta', 'Conta Contábil']
    if not all(col in df.columns for col in colunas_obrigatorias):
        st.error(f"Colunas obrigatórias ({colunas_obrigatorias}) não encontradas no arquivo de cadastro. Verifique os títulos do seu Excel.")
        return pd.DataFrame()

    # Formata Codigo_Banco e Agencia com zeros à esquerda
    if 'Codigo_Banco' in df.columns:
        df['Codigo_Banco'] = df['Codigo_Banco'].apply(lambda x: re.sub(r'\D', '', str(x)).zfill(3))
    if 'Agencia' in df.columns:
        df['Agencia'] = df['Agencia'].apply(lambda x: re.sub(r'\D', '', str(x)).zfill(4))

    # Cria a chave combinada e aplica a normalização centralizada
    df['Agencia_Normalizada'] = df['Agencia'].apply(lambda x: normalizar_numero(x).zfill(4))
    df['Conta_Cadastro_Preenchida'] = df['Conta'].apply(lambda x: normalizar_numero(x, is_conta_cadastro=True))
    chave_bruta = df['Agencia_Normalizada'].astype(str) + df['Conta_Cadastro_Preenchida'].astype(str)
    df['Conta_OFX_Normalizada'] = chave_bruta.apply(normalizar_chave_ofx)


    if 'Conta Contábil' in df.columns:
        df['Conta Contábil'] = df['Conta Contábil'].astype(str)

    # Trata a nova coluna 'Conta Contábil (-)'
    if 'Conta Contábil (-)' in df.columns:
        df['Conta Contábil (-)'] = pd.to_numeric(df['Conta Contábil (-)'], errors='coerce').fillna(0).astype(int)

    df.dropna(subset=['Conta_OFX_Normalizada'], inplace=True)
    df = df[df['Conta_OFX_Normalizada'].str.len() > 4].copy()
    print("DEBUG: DataFrame head before return in ler_cadastro_contas:\n", df.head())
    return df

# ==============================================================================
# 4. FUNÇÃO DE CARREGAMENTO DO CADASTRO DE BANCOS
# ==============================================================================

@st.cache_data(ttl=60)  # Cache de 60 segundos para permitir atualizações
def ler_bancos_associados():
    """Lê o arquivo CSV de bancos associados e retorna um DataFrame."""
    file_path = 'bancosassociados.csv'
    try:
        df = pd.read_csv(file_path)
        df['codigo_banco'] = df['codigo_banco'].astype(str)
        df['Path_Logo'] = df['arquivo_logo'].apply(lambda x: os.path.join('logos', x) if pd.notna(x) else os.path.join('logos', 'default.png'))
        return df
    except FileNotFoundError:
        st.error(f"Arquivo '{file_path}' não encontrado. Verifique se o arquivo está na pasta do projeto.")
        return pd.DataFrame(columns=['codigo_banco', 'nome_banco', 'arquivo_logo', 'Path_Logo'])
    except Exception as e:
        st.error(f"Erro ao ler o arquivo de bancos associados: {e}")
        return pd.DataFrame(columns=['codigo_banco', 'nome_banco', 'arquivo_logo', 'Path_Logo'])

# ==============================================================================
# 5. FUNÇÃO DE CARREGAMENTO DO PLANO DE CONTAS
# ==============================================================================

@st.cache_data(show_spinner="Lendo Plano de Contas...")
def ler_plano_contas_csv(uploaded_file, data_cadastro_str, delimiter=';'):
    """Lê o arquivo CSV do plano de contas, tratando o formato específico do usuário com depuração."""
    try:
        uploaded_file.seek(0)
        df = pd.read_csv(
            uploaded_file, 
            sep=delimiter, 
            header=None, 
            encoding='latin1', 
            on_bad_lines='warn'
        )
        st.info("Passo 1: DataFrame bruto lido do CSV (primeiras 10 linhas)")
        st.dataframe(df.head(10))
        
        cols_to_extract = [0, 3, 7, 11, 12, 13, 14, 15, 24]
        df_selecionado = df.iloc[:, cols_to_extract].copy()
        df_selecionado.columns = ['codigo', 'tipo_raw', 'classificacao', 'desc1', 'desc2', 'desc3', 'desc4', 'desc5', 'grau']
        st.info("Passo 2: Colunas selecionadas e renomeadas (primeiras 5 linhas)")
        st.dataframe(df_selecionado.head())

        df_limpo = df_selecionado.dropna(subset=['codigo']).copy()
        df_limpo = df_limpo[pd.to_numeric(df_limpo['codigo'], errors='coerce').notna()].copy()
        
        desc_cols = ['desc1', 'desc2', 'desc3', 'desc4', 'desc5']
        df_limpo['descricao'] = df_limpo[desc_cols].bfill(axis=1).iloc[:, 0].fillna('')

        st.info(f"Passo 3: Após remover linhas com código inválido, restam {len(df_limpo)} linhas.")
        st.dataframe(df_limpo.head())

        if df_limpo.empty:
            st.error("Nenhuma conta válida encontrada após a limpeza inicial. Verifique o formato do arquivo.")
            return pd.DataFrame()

        df_limpo['codigo'] = df_limpo['codigo'].astype(int).astype(str)
        df_limpo['classificacao'] = df_limpo['classificacao'].astype(str).str.strip()
        df_limpo['descricao'] = df_limpo['descricao'].astype(str).str.strip()
        df_limpo['grau'] = df_limpo['grau'].astype(str).str.strip()

        df_limpo['tipo_raw'] = df_limpo['tipo_raw'].fillna('A').astype(str).str.strip().str.upper()
        df_limpo['tipo'] = np.where(df_limpo['tipo_raw'] == 'S', 'Sintetico', 'Analitico')

        def get_natureza(classificacao):
            if not isinstance(classificacao, str) or len(classificacao) == 0:
                return 'Indefinida'
            primeiro_digito = classificacao.strip()[0]
            if primeiro_digito == '1':
                return 'Ativo'
            elif primeiro_digito == '2':
                return 'Passivo'
            elif primeiro_digito == '3':
                return 'Conta de Resultado'
            else:
                return 'Outra'
        df_limpo['natureza'] = df_limpo['classificacao'].apply(get_natureza)

        df_limpo['data_cadastro'] = data_cadastro_str
        df_limpo['encerrada'] = False
        df_limpo['data_encerramento'] = None

        final_cols = ['codigo', 'classificacao', 'descricao', 'tipo', 'natureza', 'grau', 'data_cadastro', 'encerrada', 'data_encerramento']
        df_final = df_limpo[final_cols].copy()
        st.info("Passo 4: DataFrame final pronto para ser importado (primeiras 5 linhas)")
        st.dataframe(df_final.head())

        return df_final

    except Exception as e:
        st.error(f"Erro crítico ao processar o arquivo do plano de contas: {e}")
        st.info("Verifique se o arquivo tem o cabeçalho nas primeiras 4 linhas e os títulos na linha 5.")
        return pd.DataFrame()

# ==============================================================================
# 6. FUNÇÕES DE CARREGAMENTO DE EXTRATO VIA PDF
# ==============================================================================

def _format_valor_brasileiro(valor_str):
    """Converte uma string de valor monetário brasileiro para float."""
    if not isinstance(valor_str, str):
        return 0.0
    # Remove espaços e o separador de milhar '.'
    valor_limpo = valor_str.strip().replace('.', '')
    # Substitui a vírgula decimal por um ponto
    valor_limpo = valor_limpo.replace(',', '.')
    try:
        return float(valor_limpo)
    except (ValueError, TypeError):
        return 0.0

@st.cache_data(show_spinner="Processando arquivo PDF...")
def importar_extrato_pdf_sicredi(file_bytes, file_name):
    """Lê um extrato de conta do Sicredi em PDF e retorna um DataFrame padronizado."""
    transactions = []
    agencia = None
    conta = None
    banco_identificador = '748' # Sicredi

    try:
        with pdfplumber.open(BytesIO(file_bytes)) as pdf:
            # Extrair agência e conta da primeira página
            page_one_text = pdf.pages[0].extract_text()
            
            coop_match = re.search(r'Cooperativa:\s*(\d+)', page_one_text)
            if coop_match:
                agencia = coop_match.group(1)

            conta_match = re.search(r'Conta:\s*([\d-]+)', page_one_text)
            if conta_match:
                conta = conta_match.group(1)

            if not agencia or not conta:
                st.error(f"Não foi possível extrair Agência/Conta do cabeçalho do PDF: {file_name}")
                return pd.DataFrame()

            # Normalizar a chave da conta
            chave_bruta = normalizar_numero(agencia) + normalizar_numero(conta)
            conta_normalizada = normalizar_chave_ofx(chave_bruta)

            # Extrair transações de todas as páginas
            for page in pdf.pages:
                table = page.extract_table()
                if not table:
                    continue

                for row in table:
                    # Pular cabeçalhos e linhas inválidas
                    if not row or len(row) < 5 or row[0] == 'Data' or (row[1] and 'SALDO ANTERIOR' in row[1]):
                        continue

                    data_str, desc, doc, valor_str, saldo_str = row[:5]
                    
                    # Validação simples da linha
                    if not data_str or not valor_str:
                        continue

                    data_lancamento = safe_parse_date(data_str, date.today())
                    valor = _format_valor_brasileiro(valor_str)
                    
                    # Gerar ID de transação único
                    id_transacao_str = f"{data_lancamento}{desc}{valor}{doc}"
                    id_transacao = hashlib.md5(id_transacao_str.encode('utf-8')).hexdigest()

                    transactions.append({
                        'Data Lançamento': data_lancamento,
                        'Valor': valor,
                        'Descrição': desc,
                        'ID Transacao': id_transacao,
                        'Tipo': 'CREDIT' if valor > 0 else 'DEBIT',
                        'Banco_OFX': banco_identificador,
                        'Conta_OFX_Normalizada': conta_normalizada
                    })

        if not transactions:
            st.error(f"Arquivo {file_name}: Nenhuma transação foi processada do PDF.")
            return pd.DataFrame()

        df = pd.DataFrame(transactions)
        df['Entrada'] = df['Valor'].apply(lambda x: x if x > 0 else 0)
        df['Saída'] = df['Valor'].apply(lambda x: abs(x) if x < 0 else 0)

        return df

    except Exception as e:
        st.error(f"Erro crítico ao processar o arquivo PDF ({file_name}): {e}")
        return pd.DataFrame()

def importar_extrato_pdf_santander(file_bytes, file_name):
    """Lê um extrato de conta do Santander em PDF e retorna um DataFrame padronizado."""
    st.info(f"Iniciando processamento do Santander para o arquivo: {file_name}")
    transactions = []
    banco_identificador = '033'

    def _format_valor_brasileiro_santander(valor_str):
        """Converte valor brasileiro para float, considerando sinal de menos no final"""
        if not isinstance(valor_str, str):
            return 0.0
        valor_str = valor_str.strip()
        is_negative = valor_str.endswith('-')
        if is_negative:
            valor_str = valor_str[:-1]
        valor_limpo = valor_str.replace('.', '').replace(',', '.')
        try:
            valor = float(valor_limpo)
            return -valor if is_negative else valor
        except:
            return 0.0

    def _is_transacao_valida(descricao):
        """Valida se a descrição é de uma transação real e não de tabela/cabeçalho"""
        desc = descricao.strip()
        # Ignorar faixas de valores (ex: "100.000 a 199.999")
        if ' a ' in desc and len(desc) < 50 and re.search(r'\d.*\s+a\s+.*\d', desc):
            # print(f"[DEBUG] TRANSACAO INVALIDA: {desc[:60]}")
            return False
        return True

    def _identificar_tipo_por_descricao(descricao, valor_str):
        """
        Identifica se é crédito ou débito baseado PRIORITARIAMENTE no sinal
        O PDF do Santander já vem com o sinal '-' correto
        """
        # PRIORIDADE 1: Se tem '-' no final, é SEMPRE débito
        if valor_str.endswith('-'):
            return 'DEBIT', True  # True = tem sinal explícito

        desc_upper = descricao.upper()

        # PRIORIDADE 2: Verificar palavras-chave que indicam CRÉDITO
        palavras_credito = [
            'PIX RECEBIDO',
            'TED RECEBIDA',
            'DOC RECEBIDO',
            'RESGATE',
            'RECEBID',
            'CREDITO',
            'DE:',  # Transferência recebida "DE: conta"
        ]

        for palavra in palavras_credito:
            if palavra in desc_upper:
                return 'CREDIT', False

        # PRIORIDADE 3: Palavras-chave que indicam DÉBITO (apenas se não tem '-')
        # IMPORTANTE: Removido "APLICACAO" e "PAGAMENTO" genéricos
        # pois o PDF já tem o sinal '-' nessas transações
        palavras_debito = [
            'PIX ENVIADO',
            'TED ENVIADA',
            'DOC ENVIADO',
            'TARIFA',
            'TAR ',
            'IOF',
            'IMPOSTO',
            'SAQUE',
            'PARA:',  # Transferência enviada "PARA: conta"
        ]

        for palavra in palavras_debito:
            if palavra in desc_upper:
                return 'DEBIT', False  # False = sem sinal explícito (inferido)

        # PRIORIDADE 4: Caso especial de transferências
        if 'TRANSF' in desc_upper:
            # Se tem "DE:" = recebida = crédito
            # Se tem "PARA:" = enviada = débito
            if 'DE:' in desc_upper:
                return 'CREDIT', False
            elif 'PARA:' in desc_upper:
                return 'DEBIT', False
            # Se não especifica, assume que valores positivos = crédito

        # PADRÃO: sem '-' e sem palavra-chave específica = CRÉDITO
        return 'CREDIT', False

    def _finalizar_valor_transacao(descricao_completa, valor_str):
        """
        Determina o valor final da transação com o sinal correto
        baseado na descrição COMPLETA (incluindo linhas de continuação)
        """
        tipo_trans, tem_sinal = _identificar_tipo_por_descricao(descricao_completa, valor_str)

        # Converter valor (sempre positivo)
        valor_abs = _format_valor_brasileiro_santander(valor_str.rstrip('-'))

        # Aplicar sinal correto
        return -valor_abs if tipo_trans == 'DEBIT' else valor_abs

    try:
        with pdfplumber.open(BytesIO(file_bytes)) as pdf:
            # Extrair ano do extrato
            current_year = str(date.today().year)
            try:
                first_page_text = pdf.pages[0].extract_text()
                match = re.search(r'janeiro[/\s]*(\d{4})', first_page_text, re.IGNORECASE)
                if not match:
                    match = re.search(r'(\d{4})', first_page_text)
                if match:
                    current_year = match.group(1)
                    st.info(f"Ano detectado: {current_year}")
            except:
                pass

            active_conta_normalizada = None
            current_trans = None  # Transação que pode continuar entre páginas
            in_movimentacao = False  # Flag global para controlar se está processando movimentação
            trans_index = 0  # Contador para garantir unicidade dos IDs

            for page_num, page in enumerate(pdf.pages):
                page_text = page.extract_text()

                # Procurar por cabeçalho de nova conta (troca de conta)
                account_match = re.search(r'Agência\s+Conta\s+Corrente\s*\n\s*(\d+)\s+([\d.-]+)', page_text, re.IGNORECASE | re.MULTILINE)
                if account_match:
                    # Salvar transação pendente da conta anterior
                    if current_trans and current_trans.get('valor_str') and active_conta_normalizada:
                        if _is_transacao_valida(current_trans['descricao']):
                            # Finalizar valor com sinal correto baseado na descrição completa
                            valor_final = _finalizar_valor_transacao(current_trans['descricao'], current_trans['valor_str'])

                            full_date = safe_parse_date(f"{current_trans['data']}/{current_year}", date.today())
                            id_transacao = hashlib.md5(
                                f"{full_date}{current_trans['descricao']}{valor_final}{active_conta_normalizada}{trans_index}".encode()
                            ).hexdigest()
                            transactions.append({
                                'Data Lançamento': full_date,
                                'Valor': valor_final,
                                'Descrição': current_trans['descricao'],
                                'ID Transacao': id_transacao,
                                'Tipo': 'CREDIT' if valor_final > 0 else 'DEBIT',
                                'Banco_OFX': banco_identificador,
                                'Conta_OFX_Normalizada': active_conta_normalizada
                            })
                            trans_index += 1

                    # Reset para nova conta
                    current_trans = None
                    in_movimentacao = False

                    agencia, conta = account_match.group(1), account_match.group(2)

                    # Normalizar agência (4 dígitos)
                    agencia_normalizada = normalizar_numero(agencia).zfill(4)

                    # Remover dígito verificador da conta (ex: 13.006124-2 -> 13006124)
                    conta_sem_digito = conta.split('-')[0] if '-' in conta else conta
                    conta_normalizada = normalizar_numero(conta_sem_digito)

                    # Criar chave normalizada
                    chave_bruta = agencia_normalizada + conta_normalizada
                    active_conta_normalizada = normalizar_chave_ofx(chave_bruta)

                    st.info(f"Página {page_num + 1}: Conta Ag {agencia} / Conta {conta} -> {active_conta_normalizada}")

                # Se não tem conta ativa, pular página
                if not active_conta_normalizada:
                    continue

                lines = page_text.split('\n')

                for line in lines:
                    line = line.strip()

                    # Detectar início da tabela (apenas na primeira vez)
                    if not in_movimentacao and 'Data' in line and 'Descrição' in line and 'Saldo (R$)' in line:
                        in_movimentacao = True
                        continue

                    # Se ainda não iniciou movimentação nesta página, pular linha
                    if not in_movimentacao:
                        continue

                    # Ignorar linhas especiais
                    if 'Créditos' in line and 'Débitos' in line:
                        continue

                    # Detectar final da movimentação (SALDO EM seguido de data)
                    if line.startswith('SALDO EM') and re.search(r'\d{2}/\d{2}', line):
                        # Final do extrato desta conta, mas não resetar in_movimentacao
                        # porque pode haver outra página desta mesma conta
                        continue

                    # Ignorar linhas especiais e RESUMOS
                    if any(kw in line for kw in [
                        'Créditos', 'Débitos', 'SALDO EM', 'Extrato_PJ', 'Pagina:', 'BALP_',
                        'Depósitos / Transferências', 'Pagamentos / Transferências',
                        'Outros Créditos', 'Outros Débitos',
                        '(=) Saldo', '(+) Total', '(-) Total',
                        'Resumo -', 'Agência Conta Corrente',
                        'Saldos por Período', 'Saldo de', 'Saldo Bloqueio',
                        'Saldo Disponível', 'Provisão de Encargos',
                        'Investimentos com Resgate', 'Tipo de Aplicação',
                        'Produto', 'Saldo Bruto', 'Pacote de Serviços',
                        'SALARIO MINIMO',
                        # Tabelas de índices econômicos
                        'IBOVESPA', 'IGPM', 'INCC', 'INPC', 'IPCA',
                        'CDI JANEIRO', 'TR JANEIRO', 'POUPANCA JANEIRO',
                        'EURO JANEIRO', 'DOLAR COMERCIAL',
                        'Índices Econômicos',
                        # Linhas de pontuação/faixas
                        '100.000 a', '200.000', 'Pontos',
                        'COMPOSIÇÃO DA PONTUAÇÃO',
                        # Tabelas de investimentos
                        'Acumulado Mês', 'Valor original',
                        'Rendimento Total', 'resgatado acrescido',
                        # Cabeçalhos
                        'Referência', '% Mês', '% Ano',
                        'Data Descrição Nº Documento', 'Movimentos (R$) Saldo (R$)',
                        'Data Descri��o N� Documento',
                        # Tabelas de investimentos detalhadas
                        'Rendimento de cada resgate', 'IOF sobre', 'IR sobre',
                        'Posição Consolidada', 'Aplicações resgatadas',
                        'CDB ContaMax', 'Rendimento apurado',
                        'Base IR Fonte', 'IOF R$', 'IR R$',
                        # Outros textos de relatório
                        'conforme legislação', 'Para mais informações',
                        'consulte o nosso site', 'saldo médio de investimentos',
                        'tempo de relacionamento', 'débito automático',
                        # Textos de rodapé e avisos
                        'EXTRATO CONSOLIDADO INTELIGENTE', 'Os percentuais apresentados',
                        'Sesuaempresa', 'produtocontratado', 'encargos. Desconsidere',
                        'Valores deduzidos do saldo disponível',
                        'PACOTE BUSINESS', 'PACOTE INSTITUICAO',
                        'MANUTENCAO DE CONTA CORRENTE', 'TRANSF ENTRE CONTAS',
                        'CANAIS ELETRONICOS', 'OUTRAS TARIFAS',
                        'Valor da Mensalidade', 'Status do Débito',
                        'Programa de Relacionamento', 'PONTUAÇÃO ATUAL',
                        'PRODUTOS PONTOS', 'DEPOSITOS A PRAZO',
                        'Sua segurança é importante',
                        'Cuidado com o Golpe', 'Facilidade na contratação',
                        'Voc� e Seu Dinheiro', 'Capital de Giro',
                    ]):
                        continue

                    # Ignorar linhas de tabelas de investimentos (formato: DD/JAN/AA)
                    if re.search(r'\d{2}/[A-Z]{3}/\d{2}', line):
                        continue

                    # Ignorar linhas com múltiplos "0,00" seguidos (tabelas de investimento)
                    if line.count('0,00') >= 3:
                        continue

                    # Ignorar apenas linhas de tabelas com números de documento SEGUIDOS de múltiplos valores
                    # (não bloquear PIX/TED que têm CNPJ/CPF na descrição)
                    if re.search(r'\d{11,}.*?\d{1,3}(?:\.\d{3})*,\d{2}.*?\d{1,3}(?:\.\d{3})*,\d{2}', line):
                        continue

                    # Detectar início de transação (DD/MM)
                    date_match = re.match(r'^(\d{2}/\d{2})\s+(.+)', line)

                    if date_match:
                        # Ignorar linhas que parecem ser títulos ou totalizadores
                        resto_linha = date_match.group(2)
                        if any(kw in resto_linha for kw in [
                            'DI CDB DI', 'TOTAL GERAL', 'LCI LCA',
                            '100,00%', 'Movimentação Mensal',
                            'Anterior R$', 'resgatado no mês',
                        ]):
                            continue

                        # DEBUG
                        # if current_trans:
                        #     # print(f"[DEBUG] Tentando salvar anterior: tem valor_str={bool(current_trans.get('valor_str'))}, descricao={current_trans.get('descricao', '')[:40]}")

                        # Salvar transação anterior se existir
                        if current_trans and current_trans.get('valor_str'):
                            # Validação: apenas salvar se for transação válida
                            valida = _is_transacao_valida(current_trans['descricao'])
                            # # print(f"[DEBUG] Validacao={valida} para: {current_trans['descricao'][:60]}")
                            if valida:
                                try:
                                    # Finalizar valor com sinal correto baseado na descrição completa
                                    valor_final = _finalizar_valor_transacao(current_trans['descricao'], current_trans['valor_str'])

                                    full_date = safe_parse_date(f"{current_trans['data']}/{current_year}", date.today())
                                    id_transacao = hashlib.md5(
                                        f"{full_date}{current_trans['descricao']}{valor_final}{active_conta_normalizada}{trans_index}".encode()
                                    ).hexdigest()
                                    transactions.append({
                                        'Data Lançamento': full_date,
                                        'Valor': valor_final,
                                        'Descrição': current_trans['descricao'],
                                        'ID Transacao': id_transacao,
                                        'Tipo': 'CREDIT' if valor_final > 0 else 'DEBIT',
                                        'Banco_OFX': banco_identificador,
                                        'Conta_OFX_Normalizada': active_conta_normalizada
                                    })
                                    trans_index += 1
                                    # print(f"[DEBUG] SALVOU COM DATA trans #{trans_index}: {current_trans['data']} | {valor_final}")
                                except Exception as e:
                                    # print(f"[DEBUG] ERRO ao salvar: {e}")
                                    pass

                        # Iniciar nova transação
                        data_str = date_match.group(1)
                        resto = date_match.group(2)
                        valor_match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2}-?)\s*(?:(\d{1,3}(?:\.\d{3})*,\d{2}))?$', resto)

                        descricao = resto
                        valor_str = None

                        if valor_match:
                            valor_str = valor_match.group(1)
                            descricao = resto[:valor_match.start()].strip()
                            # print(f"[DEBUG] NOVA COM DATA: {data_str} | {valor_str} | {descricao[:40]}")

                        current_trans = {'data': data_str, 'descricao': descricao, 'valor_str': valor_str}

                    else:
                        # Linha de continuação
                        if current_trans:
                            # Ignorar linhas de tabelas que têm valores mas não são transações
                            if any(kw in line for kw in [
                                'DI CDB DI', 'TOTAL GERAL', 'Tipo de Aplicação',
                                'Saldo Bruto', '100,00%', 'ContaMax Empresarial',
                                'Investimentos', 'LCI LCA',
                                '500.000 a', '999.999', 'EURO 31/01', 'DOLAR',
                                'Limite para Débito (R$)', 'Dia Judicial Resgate',
                                '72,74 72,74', '45,69 45,69',  # padrões de valores duplicados
                                'Extrato_PJ', 'BALP_', 'Pagina:', 'Data Descrição',
                                'Movimentos (R$)', 'Saldo (R$)', 'EXTRATO CONSOLIDADO',
                                'janeiro/2023', 'Conta Corrente',
                            ]):
                                continue

                            # Se a linha contém palavras-chave de seções, não adicionar à descrição
                            if any(keyword in line for keyword in [
                                'Desconsidere esta informação',
                                'n�o haver� cobran�a',
                                'sujeito�cobran�a',
                                'Conta Corrente',
                                'Saldo Bruto',
                                'SALARIO MINIMO',
                                'Data Descrição',
                                'N� Documento',
                                'Movimentos (R$)',
                                'Saldo (R$)'
                            ]):
                                # Finalizar transação atual se tiver valor
                                if current_trans.get('valor_str'):
                                    if _is_transacao_valida(current_trans['descricao']):
                                        # Finalizar valor com sinal correto baseado na descrição completa
                                        valor_final = _finalizar_valor_transacao(current_trans['descricao'], current_trans['valor_str'])

                                        full_date = safe_parse_date(f"{current_trans['data']}/{current_year}", date.today())
                                        id_transacao = hashlib.md5(
                                            f"{full_date}{current_trans['descricao']}{valor_final}{active_conta_normalizada}{trans_index}".encode()
                                        ).hexdigest()
                                        transactions.append({
                                            'Data Lançamento': full_date,
                                            'Valor': valor_final,
                                            'Descrição': current_trans['descricao'],
                                            'ID Transacao': id_transacao,
                                            'Tipo': 'CREDIT' if valor_final > 0 else 'DEBIT',
                                            'Banco_OFX': banco_identificador,
                                            'Conta_OFX_Normalizada': active_conta_normalizada
                                        })
                                        trans_index += 1
                                    current_trans = None
                                continue

                            valor_match = re.search(r'(\d{1,3}(?:\.\d{3})*,\d{2}-?)\s*(?:(\d{1,3}(?:\.\d{3})*,\d{2}))?$', line)

                            if valor_match:
                                valor_str = valor_match.group(1)
                                desc_parte = line[:valor_match.start()].strip()

                                # DEBUG
                                # print(f"[DEBUG] Linha sem data com valor: {desc_parte[:40]} = {valor_str}")

                                # Salvar transação anterior antes de iniciar nova
                                if current_trans.get('valor_str'):
                                    if _is_transacao_valida(current_trans['descricao']):
                                        # Finalizar valor com sinal correto baseado na descrição completa
                                        valor_final = _finalizar_valor_transacao(current_trans['descricao'], current_trans['valor_str'])

                                        full_date = safe_parse_date(f"{current_trans['data']}/{current_year}", date.today())
                                        id_transacao = hashlib.md5(
                                            f"{full_date}{current_trans['descricao']}{valor_final}{active_conta_normalizada}{trans_index}".encode()
                                        ).hexdigest()
                                        transactions.append({
                                            'Data Lançamento': full_date,
                                            'Valor': valor_final,
                                            'Descrição': current_trans['descricao'],
                                            'ID Transacao': id_transacao,
                                            'Tipo': 'CREDIT' if valor_final > 0 else 'DEBIT',
                                            'Banco_OFX': banco_identificador,
                                            'Conta_OFX_Normalizada': active_conta_normalizada
                                        })
                                        trans_index += 1
                                        # print(f"[DEBUG] SALVOU trans #{trans_index}: {current_trans['data']} | {valor_final}")

                                # Iniciar nova transação na mesma data
                                current_trans = {'data': current_trans['data'], 'descricao': desc_parte, 'valor_str': valor_str}
                                # print(f"[DEBUG] NOVA trans mesma data: {current_trans['data']} | {desc_parte[:30]}")
                            else:
                                current_trans['descricao'] += ' ' + line

                # NÃO salvar transação no final da página
                # Ela pode continuar na próxima página
                # A transação só será salva quando encontrar:
                # 1. Uma nova transação com data
                # 2. Troca de conta
                # 3. Final do documento

            # Salvar última transação pendente (final do documento)
            if current_trans and current_trans.get('valor_str') and active_conta_normalizada:
                if _is_transacao_valida(current_trans['descricao']):
                    # Finalizar valor com sinal correto baseado na descrição completa
                    valor_final = _finalizar_valor_transacao(current_trans['descricao'], current_trans['valor_str'])

                    full_date = safe_parse_date(f"{current_trans['data']}/{current_year}", date.today())
                    id_transacao = hashlib.md5(
                        f"{full_date}{current_trans['descricao']}{valor_final}{active_conta_normalizada}{trans_index}".encode()
                    ).hexdigest()
                    transactions.append({
                        'Data Lançamento': full_date,
                        'Valor': valor_final,
                        'Descrição': current_trans['descricao'],
                        'ID Transacao': id_transacao,
                        'Tipo': 'CREDIT' if valor_final > 0 else 'DEBIT',
                        'Banco_OFX': banco_identificador,
                        'Conta_OFX_Normalizada': active_conta_normalizada
                    })
                    trans_index += 1

        if not transactions:
            st.error(f"Nenhuma transação foi extraída do arquivo {file_name}")
            return pd.DataFrame()

        st.success(f"{len(transactions)} transações do Santander extraídas do arquivo {file_name}")

        df = pd.DataFrame(transactions)
        df['Entrada'] = df['Valor'].apply(lambda x: x if x > 0 else 0)
        df['Saída'] = df['Valor'].apply(lambda x: abs(x) if x < 0 else 0)
        df['Data Processamento'] = df['Data Lançamento']

        return df

    except Exception as e:
        st.error(f"Erro crítico ao processar o PDF do Santander: {e}")
        st.exception(e)
        return pd.DataFrame()


# ==============================================================================
# 7. FUNÇÕES DE IMPORTAÇÃO DE CSV BRADESCO
# ==============================================================================

def importar_extrato_csv_bradesco(file_bytes, file_name):
    """
    Lê um arquivo CSV do Bradesco, extrai transações e retorna um DataFrame padronizado.

    Formato esperado:
    - Linha 1: vazia
    - Linha 2: ;Extrato de: Agência: XXX  Conta: XXXXX-X
    - Linha 3: Data;Lançamento;Dcto.;Crédito (R$);Débito (R$);Saldo (R$)
    - Linhas seguintes: transações
    - Linha de Total
    - Linhas de rodapé
    """
    import hashlib
    from utils import normalizar_chave_ofx

    try:
        # Tentar diferentes encodings
        encodings = ['latin-1', 'cp1252', 'utf-8', 'iso-8859-1']
        content = None

        for encoding in encodings:
            try:
                content = file_bytes.decode(encoding)
                break
            except:
                continue

        if content is None:
            st.error(f"Erro ao decodificar o arquivo {file_name}. Formato não reconhecido.")
            return pd.DataFrame()

        # Normalizar quebras de linha (\r\n, \r, \n) para \n
        content = content.replace('\r\n', '\n').replace('\r', '\n')

        # Adicionar quebras de linha antes de cada data (formato DD/MM/YYYY) se estiverem faltando
        # Isso corrige arquivos onde as transações estão todas na mesma linha
        content = re.sub(r'(\d{2}/\d{2}/\d{4})', r'\n\1', content)

        lines = content.split('\n')

        # Verificar se o arquivo tem dados (procurar por linha com "Extrato de:")
        tem_extrato = False
        for line in lines:
            if 'Extrato de:' in line or 'Agência:' in line or 'Ag' in line:
                tem_extrato = True
                break

        if not tem_extrato:
            # Arquivo vazio ou sem movimentações
            st.info(f"⚠️ Arquivo {file_name} não contém extratos (período sem movimentações)")
            return pd.DataFrame()

        # Extrair agência e conta procurando nas primeiras linhas
        # Formato pode variar: ;Extrato de: Agência: XXX  Conta: XXXXX-X
        agencia = ''
        conta = ''

        # Procurar por "Extrato de:" nas primeiras 5 linhas
        texto_extrato = ''
        for i in range(min(5, len(lines))):
            linha = lines[i]
            if 'Extrato de:' in linha or 'Agência:' in linha or 'Ag' in linha:
                texto_extrato = linha
                break

        if texto_extrato:
            # Extrair agência
            match_ag = re.search(r'Ag[êeéè]ncia:\s*(\d+)', texto_extrato, re.IGNORECASE)
            if match_ag:
                agencia = match_ag.group(1)

            # Extrair conta (com ou sem dígito)
            match_ct = re.search(r'Conta:\s*([\d-]+)', texto_extrato, re.IGNORECASE)
            if match_ct:
                conta_completa = match_ct.group(1)
                # Remover apenas o hífen, mantendo todos os dígitos (ex: "10-8" -> "108")
                conta = conta_completa.replace('-', '')

        if not agencia or not conta:
            st.warning(f"⚠️ Não foi possível extrair agência/conta do arquivo {file_name}")
            st.info(f"Conteúdo do arquivo: {lines[:3]}")
            return pd.DataFrame()

        conta_ofx = f"{agencia}-{conta}" if agencia and conta else conta

        # Processar transações (começam na linha 3 ou posterior)
        transactions = []

        # Encontrar a linha com o cabeçalho de colunas (procurar por "Data" no início)
        header_line_index = -1
        for i, line in enumerate(lines):
            line_clean = line.strip()
            if line_clean.startswith('Data;') or (line_clean.startswith('Data') and ';' in line_clean):
                header_line_index = i
                break

        if header_line_index == -1:
            st.warning(f"Cabeçalho de dados não encontrado no arquivo {file_name}")
            st.info(f"Primeiras 5 linhas do arquivo: {lines[:5]}")
            return pd.DataFrame()

        for line in lines[header_line_index + 1:]:  # Começar após o cabeçalho
            line = line.strip()

            # Parar nas linhas de total ou rodapé
            if not line or line.startswith('Total') or line.startswith(';Saldos') or line.startswith(';N') or line.startswith(';'):
                continue

            # Verificar se é uma linha válida com data
            parts = line.split(';')
            if len(parts) < 6:
                continue

            data_str = parts[0].strip()

            # Validar se a primeira coluna é uma data válida (DD/MM/YYYY)
            if not re.match(r'\d{2}/\d{2}/\d{4}', data_str):
                continue

            # Ignorar linha de SALDO ANTERIOR
            lancamento = parts[1].strip()
            if 'SALDO ANTERIOR' in lancamento.upper():
                continue

            dcto = parts[2].strip()
            credito_str = parts[3].strip()
            debito_str = parts[4].strip()
            saldo_str = parts[5].strip() if len(parts) > 5 else ''

            # Converter valores brasileiros (1.234,56) para float
            def parse_valor_br(valor_str):
                if not valor_str or valor_str == '':
                    return 0.0
                # Remover pontos de milhares e substituir vírgula por ponto
                valor_str = valor_str.replace('.', '').replace(',', '.')
                try:
                    return float(valor_str)
                except:
                    return 0.0

            credito = parse_valor_br(credito_str)
            debito = parse_valor_br(debito_str)

            # Coluna 4 (Crédito) = valores POSITIVOS (entrada de dinheiro)
            # Coluna 5 (Débito) = valores NEGATIVOS (saída de dinheiro)
            # Nota: débitos podem vir com sinal negativo no CSV (ex: -100,00)
            if credito > 0:
                valor = credito  # Positivo
                tipo_transacao = 'CREDIT'
            elif debito != 0:  # Débito pode ser negativo ou positivo no arquivo
                # Garante que o valor final seja sempre negativo para débitos
                valor = debito if debito < 0 else -debito
                tipo_transacao = 'DEBIT'
            else:
                valor = 0.0
                tipo_transacao = 'CREDIT'

            # Converter data
            try:
                data_obj = pd.to_datetime(data_str, format='%d/%m/%Y').date()
            except:
                continue

            # Criar ID único para a transação (hash de data + descrição + valor + conta)
            id_str = f"{data_obj}{lancamento}{valor}{conta_ofx}"
            id_transacao = hashlib.md5(id_str.encode()).hexdigest()

            # Normalizar a conta OFX (remover hífens e espaços)
            conta_normalizada = normalizar_chave_ofx(conta_ofx)

            transactions.append({
                'Data Lançamento': data_obj,
                'Data Processamento': data_obj,  # Mesmo valor da data de lançamento
                'Descrição': lancamento,
                'Valor': valor,
                'ID Transacao': id_transacao,
                'Tipo': tipo_transacao,
                'Banco_OFX': '237',  # Código do Bradesco
                'Conta_OFX_Normalizada': conta_normalizada,
                'Arquivo': file_name,
                'Entrada': credito,
                'Saída': debito
            })

        if not transactions:
            st.warning(f"Nenhuma transação encontrada no arquivo {file_name}")
            return pd.DataFrame()

        df = pd.DataFrame(transactions)

        # Debug: contar créditos e débitos
        num_creditos = len([t for t in transactions if t['Tipo'] == 'CREDIT'])
        num_debitos = len([t for t in transactions if t['Tipo'] == 'DEBIT'])
        total_creditos = sum([t['Valor'] for t in transactions if t['Tipo'] == 'CREDIT'])
        total_debitos = sum([abs(t['Valor']) for t in transactions if t['Tipo'] == 'DEBIT'])

        st.success(f"✅ {len(transactions)} transações importadas do arquivo {file_name}")
        st.info(f"📊 Créditos: {num_creditos} (R$ {total_creditos:,.2f}) | Débitos: {num_debitos} (R$ {total_debitos:,.2f})")

        return df

    except Exception as e:
        st.error(f"Erro ao processar CSV do Bradesco {file_name}: {e}")
        st.exception(e)
        return pd.DataFrame()


def importar_multiplos_csvs_bradesco(uploaded_files):
    """Importa múltiplos arquivos CSV do Bradesco e retorna um DataFrame consolidado."""
    all_dfs = []

    for uploaded_file in uploaded_files:
        file_bytes = uploaded_file.read()
        file_name = uploaded_file.name

        df = importar_extrato_csv_bradesco(file_bytes, file_name)
        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    df_final = pd.concat(all_dfs, ignore_index=True)
    df_final = df_final.sort_values('Data Lançamento').reset_index(drop=True)

    return df_final
