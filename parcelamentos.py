"""
Módulo de Parcelamentos da Receita Federal / PGFN / Procuradoria
Inclui parser de PDF do e-CAC e funções de controle de parcelamentos
"""
import re
import os
import pdfplumber
import pandas as pd
from datetime import datetime
from typing import Dict, List, Tuple, Optional


def parse_valor_brasileiro(valor_str: str) -> float:
    """Converte valor em formato brasileiro (1.234,56) para float."""
    if not valor_str or valor_str.strip() == '-':
        return 0.0
    try:
        # Remove pontos de milhar e substitui vírgula por ponto
        valor_limpo = valor_str.strip().replace('.', '').replace(',', '.')
        return float(valor_limpo)
    except (ValueError, AttributeError):
        return 0.0


def parse_data_brasileira(data_str: str) -> Optional[str]:
    """Converte data em formato brasileiro (dd/mm/yyyy) para formato ISO (yyyy-mm-dd)."""
    if not data_str or data_str.strip() == '-':
        return None
    try:
        data = datetime.strptime(data_str.strip(), '%d/%m/%Y')
        return data.strftime('%Y-%m-%d')
    except ValueError:
        return None


def extrair_texto_pdf(pdf_path: str) -> str:
    """Extrai todo o texto do PDF."""
    texto_completo = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            texto = page.extract_text()
            if texto:
                texto_completo += texto + "\n"
    return texto_completo


def extrair_texto_xps(xps_path: str) -> str:
    """Extrai texto de arquivo XPS (usado pelo PERT/e-CAC)."""
    import zipfile

    texto_completo = ""
    try:
        with zipfile.ZipFile(xps_path, 'r') as zip_ref:
            # XPS é um ZIP com XMLs
            for name in zip_ref.namelist():
                if name.endswith('.fpage'):
                    content = zip_ref.read(name).decode('utf-8', errors='ignore')
                    # Extrai texto das tags Glyphs (UnicodeString)
                    matches = re.findall(r'UnicodeString="([^"]*)"', content)
                    texto_completo += ' '.join(matches) + '\n'
    except Exception as e:
        raise Exception(f"Erro ao ler arquivo XPS: {str(e)}")

    return texto_completo


def parse_arquivo_parcelamento(arquivo_path: str) -> Dict:
    """
    Faz o parse de arquivo de parcelamento (PDF ou XPS).
    Detecta automaticamente o tipo pelo extensão.
    """
    extensao = os.path.splitext(arquivo_path)[1].lower()

    if extensao == '.pdf':
        return parse_extrato_parcelamento_ecac(arquivo_path)
    elif extensao == '.xps':
        return parse_extrato_parcelamento_xps(arquivo_path)
    else:
        return {'erros': [f'Formato de arquivo não suportado: {extensao}']}


def parse_extrato_parcelamento_xps(xps_path: str) -> Dict:
    """
    Faz o parse do XPS de Extrato de Parcelamento (usado pelo PERT).
    """
    resultado = {
        'dados_parcelamento': {},
        'resumo_divida': {},
        'debitos': [],
        'parcelas': [],
        'pagamentos': [],
        'erros': []
    }

    try:
        texto = extrair_texto_xps(xps_path)

        if not texto or len(texto) < 100:
            resultado['erros'].append("Não foi possível extrair texto do arquivo XPS. Tente salvar como PDF.")
            return resultado

        # Usa as mesmas funções de extração do PDF
        resultado['dados_parcelamento'] = extrair_dados_parcelamento(texto)
        resultado['resumo_divida'] = extrair_resumo_divida(texto)
        resultado['debitos'] = extrair_debitos_do_texto(texto)
        resultado['parcelas'] = extrair_parcelas_do_texto(texto)
        resultado['pagamentos'] = extrair_pagamentos_do_texto(texto)

        # Marca como PERT se detectado
        if 'pert' in texto.lower():
            resultado['dados_parcelamento']['modalidade'] = 'PERT'

        # Fallbacks
        if not resultado['dados_parcelamento'].get('qtd_parcelas') and resultado['parcelas']:
            resultado['dados_parcelamento']['qtd_parcelas'] = len(resultado['parcelas'])

        # Pega valor_parcela da última parcela paga (valor corrigido monetariamente)
        if not resultado['dados_parcelamento'].get('valor_parcela') and resultado['parcelas']:
            parcelas_pagas = [p for p in resultado['parcelas'] if p.get('situacao') == 'Paga']
            if parcelas_pagas:
                ultima_paga = parcelas_pagas[-1]
                resultado['dados_parcelamento']['valor_parcela'] = ultima_paga.get('valor_pago') or ultima_paga.get('valor_originario')
            elif resultado['parcelas']:
                resultado['dados_parcelamento']['valor_parcela'] = resultado['parcelas'][0].get('valor_originario')

        # Para PERT: Calcula valor_total_consolidado a partir das parcelas
        if not resultado['resumo_divida'].get('valor_total_consolidado') and resultado['parcelas']:
            total_parcelas = sum(p.get('valor_originario', 0) for p in resultado['parcelas'])
            if total_parcelas > 0:
                resultado['resumo_divida']['valor_total_consolidado'] = total_parcelas

        # Se ainda não tem valor_total_consolidado, usa saldo_devedor como fallback
        if not resultado['resumo_divida'].get('valor_total_consolidado'):
            if resultado['resumo_divida'].get('saldo_devedor'):
                resultado['resumo_divida']['valor_total_consolidado'] = resultado['resumo_divida']['saldo_devedor']

        # Calcula progresso das parcelas (verifica data de vencimento para Devedora)
        if resultado['parcelas']:
            resultado['dados_parcelamento']['qtd_pagas'] = 0
            resultado['dados_parcelamento']['qtd_vencidas'] = 0
            resultado['dados_parcelamento']['qtd_a_vencer'] = 0

            from datetime import datetime
            hoje = datetime.now().date()

            for p in resultado['parcelas']:
                situacao = p.get('situacao', '')
                if situacao == 'Paga':
                    resultado['dados_parcelamento']['qtd_pagas'] += 1
                elif situacao == 'Vencida':
                    # Já está explícito como Vencida (PGFN)
                    resultado['dados_parcelamento']['qtd_vencidas'] += 1
                elif situacao == 'A vencer':
                    # Já está explícito como A vencer
                    resultado['dados_parcelamento']['qtd_a_vencer'] += 1
                elif situacao == 'Devedora':
                    # Verifica pela data de vencimento se é vencida ou a vencer
                    data_venc_str = p.get('data_vencimento')
                    if data_venc_str:
                        try:
                            data_venc = datetime.strptime(data_venc_str, '%Y-%m-%d').date()
                            if data_venc < hoje:
                                resultado['dados_parcelamento']['qtd_vencidas'] += 1
                            else:
                                resultado['dados_parcelamento']['qtd_a_vencer'] += 1
                        except:
                            resultado['dados_parcelamento']['qtd_vencidas'] += 1
                    else:
                        resultado['dados_parcelamento']['qtd_vencidas'] += 1

        # Se data_adesao não foi encontrada, usa data_inicio como fallback
        if not resultado['dados_parcelamento'].get('data_adesao'):
            if resultado['dados_parcelamento'].get('data_inicio'):
                resultado['dados_parcelamento']['data_adesao'] = resultado['dados_parcelamento']['data_inicio']

        # Para PERT/PGFN, data_consolidacao = data_adesao (negociação já é consolidada)
        if not resultado['dados_parcelamento'].get('data_consolidacao'):
            if resultado['dados_parcelamento'].get('data_adesao'):
                resultado['dados_parcelamento']['data_consolidacao'] = resultado['dados_parcelamento']['data_adesao']

    except Exception as e:
        resultado['erros'].append(f"Erro ao processar XPS: {str(e)}")

    return resultado


def extrair_tabelas_pdf(pdf_path: str) -> List[List[List[str]]]:
    """Extrai todas as tabelas do PDF."""
    todas_tabelas = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tabelas = page.extract_tables()
            if tabelas:
                todas_tabelas.extend(tabelas)
    return todas_tabelas


def parse_extrato_parcelamento_ecac(pdf_path: str) -> Dict:
    """
    Faz o parse do PDF de Extrato de Parcelamento do e-CAC.
    Retorna um dicionário com todas as informações extraídas.
    """
    resultado = {
        'dados_parcelamento': {},
        'resumo_divida': {},
        'debitos': [],
        'parcelas': [],
        'pagamentos': [],
        'erros': []
    }

    try:
        texto = extrair_texto_pdf(pdf_path)

        # Extrai dados gerais do parcelamento
        resultado['dados_parcelamento'] = extrair_dados_parcelamento(texto)
        resultado['resumo_divida'] = extrair_resumo_divida(texto)

        # Extrai débitos, parcelas e pagamentos diretamente do texto
        # (pdfplumber não extrai bem as tabelas deste PDF)
        resultado['debitos'] = extrair_debitos_do_texto(texto)
        resultado['parcelas'] = extrair_parcelas_do_texto(texto)
        resultado['pagamentos'] = extrair_pagamentos_do_texto(texto)

        # Se não conseguiu extrair qtd_parcelas, usa a quantidade real de parcelas extraídas
        if not resultado['dados_parcelamento'].get('qtd_parcelas') and resultado['parcelas']:
            resultado['dados_parcelamento']['qtd_parcelas'] = len(resultado['parcelas'])

        # Se não conseguiu extrair valor_parcela, pega da última parcela paga
        if not resultado['dados_parcelamento'].get('valor_parcela') and resultado['parcelas']:
            # Procura a última parcela paga (valor corrigido monetariamente)
            parcelas_pagas = [p for p in resultado['parcelas'] if p.get('situacao') == 'Paga']
            if parcelas_pagas:
                ultima_paga = parcelas_pagas[-1]
                # Usa valor_pago se disponível, senão valor_originario
                resultado['dados_parcelamento']['valor_parcela'] = ultima_paga.get('valor_pago') or ultima_paga.get('valor_originario')
            elif resultado['parcelas']:
                # Fallback: primeira parcela se não houver nenhuma paga
                resultado['dados_parcelamento']['valor_parcela'] = resultado['parcelas'][0].get('valor_originario')

        # Para PERT e outros formatos sem detalhamento de principal/multa/juros:
        # Calcula valor_total_consolidado a partir das parcelas
        if not resultado['resumo_divida'].get('valor_total_consolidado') and resultado['parcelas']:
            total_parcelas = sum(p.get('valor_originario', 0) for p in resultado['parcelas'])
            if total_parcelas > 0:
                resultado['resumo_divida']['valor_total_consolidado'] = total_parcelas

        # Se ainda não tem valor_total_consolidado, usa saldo_devedor como fallback
        if not resultado['resumo_divida'].get('valor_total_consolidado'):
            if resultado['resumo_divida'].get('saldo_devedor'):
                resultado['resumo_divida']['valor_total_consolidado'] = resultado['resumo_divida']['saldo_devedor']

        # Calcula progresso das parcelas (verifica data de vencimento para Devedora)
        if resultado['parcelas']:
            resultado['dados_parcelamento']['qtd_pagas'] = 0
            resultado['dados_parcelamento']['qtd_vencidas'] = 0
            resultado['dados_parcelamento']['qtd_a_vencer'] = 0

            from datetime import datetime
            hoje = datetime.now().date()

            for p in resultado['parcelas']:
                situacao = p.get('situacao', '')
                if situacao == 'Paga':
                    resultado['dados_parcelamento']['qtd_pagas'] += 1
                elif situacao == 'Vencida':
                    # Já está explícito como Vencida (PGFN)
                    resultado['dados_parcelamento']['qtd_vencidas'] += 1
                elif situacao == 'A vencer':
                    # Já está explícito como A vencer
                    resultado['dados_parcelamento']['qtd_a_vencer'] += 1
                elif situacao == 'Devedora':
                    # Verifica pela data de vencimento se é vencida ou a vencer
                    data_venc_str = p.get('data_vencimento')
                    if data_venc_str:
                        try:
                            data_venc = datetime.strptime(data_venc_str, '%Y-%m-%d').date()
                            if data_venc < hoje:
                                resultado['dados_parcelamento']['qtd_vencidas'] += 1
                            else:
                                resultado['dados_parcelamento']['qtd_a_vencer'] += 1
                        except:
                            resultado['dados_parcelamento']['qtd_vencidas'] += 1
                    else:
                        resultado['dados_parcelamento']['qtd_vencidas'] += 1

        # Se data_adesao não foi encontrada, usa data_inicio como fallback
        if not resultado['dados_parcelamento'].get('data_adesao'):
            if resultado['dados_parcelamento'].get('data_inicio'):
                resultado['dados_parcelamento']['data_adesao'] = resultado['dados_parcelamento']['data_inicio']

        # Para PERT/PGFN, data_consolidacao = data_adesao (negociação já é consolidada)
        if not resultado['dados_parcelamento'].get('data_consolidacao'):
            if resultado['dados_parcelamento'].get('data_adesao'):
                resultado['dados_parcelamento']['data_consolidacao'] = resultado['dados_parcelamento']['data_adesao']

    except Exception as e:
        resultado['erros'].append(f"Erro ao processar PDF: {str(e)}")

    return resultado


def extrair_debitos_do_texto(texto: str) -> List[Dict]:
    """
    Extrai a lista de débitos diretamente do texto do PDF.
    Formato e-CAC: "0561-07 março/2024 19/04/2024 BRL 19.184,94 19.184,94 3.836,98 0,00 23.021,92 ..."
    """
    debitos = []

    # Padrão para linha de débito:
    # Código (0000-00) + Período (mês/ano) + Vencimento (dd/mm/yyyy) + BRL + valores
    pattern = r'(\d{4}-\d{2})\s+(\w+/\d{4})\s+(\d{2}/\d{2}/\d{4})\s+BRL\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)'

    matches = re.findall(pattern, texto, re.IGNORECASE)

    for match in matches:
        codigo_receita = match[0]
        periodo_apuracao = match[1]
        data_vencimento = match[2]
        saldo_originario = parse_valor_brasileiro(match[3])
        valor_principal = parse_valor_brasileiro(match[4])
        valor_multa = parse_valor_brasileiro(match[5])
        valor_juros = parse_valor_brasileiro(match[6])
        valor_consolidado = parse_valor_brasileiro(match[7])

        debito = {
            'codigo_receita': codigo_receita,
            'periodo_apuracao': periodo_apuracao,
            'data_vencimento': parse_data_brasileira(data_vencimento),
            'saldo_originario': saldo_originario,
            'valor_principal': valor_principal,
            'valor_multa': valor_multa,
            'valor_juros': valor_juros,
            'valor_total': valor_consolidado
        }
        debitos.append(debito)

    return debitos


def extrair_parcelas_do_texto(texto: str) -> List[Dict]:
    """
    Extrai a lista de parcelas diretamente do texto do PDF.
    Formato e-CAC: "1 26/04/2024 3.804,16 0,00 Paga"
                   "8 29/11/2024 3.804,16 4.216,91 Devedora"
    Formato PERT:  "1 31/10/2017 125.659,86 31/08/2018 127.792,36 0,00 Liquidada 12"
                   "97 30/01/2026 13.242,37 - 0,00 13.242,37 Devedora 0"
    Formato PGFN:  "0001 Prestaçao Básica 4.424,55 0,00 0,00 0,00 0,00 0,00 31/05/2017 Quitada"
                   "0002 Prestação 4.424,55 2.959,64 591,92 470,74 402,23 4.424,55 30/06/2017 Vencida"
    """
    parcelas = []

    # Padrão 1: Formato e-CAC (Parcelamento Simplificado)
    # Número + Vencimento (dd/mm/yyyy) + Valor originário + Saldo atualizado + Situação
    pattern1 = r'^(\d{1,3})\s+(\d{2}/\d{2}/\d{4})\s+([\d.,]+)\s+([\d.,]+)\s+(Paga|Devedora|A vencer|Vencida)$'

    # Padrão 2: Formato PERT
    # Parcela + Dt.Venc + Valor Devido + Dt.Pag + Valor Pago + Saldo + Situação + Qtd + opcionalmente " -"
    # Ex: "1 31/10/2017 125.659,86 31/08/2018 127.792,36 0,00 Liquidada 12 -"
    # Ex: "97 30/01/2026 13.242,37 - 0,00 13.242,37 Devedora 0"
    pattern2 = r'^(\d{1,3})\s+(\d{2}/\d{2}/\d{4})\s+([\d.,]+)\s+(?:(\d{2}/\d{2}/\d{4})|-)\s+([\d.,]+)\s+([\d.,]+)\s+(Liquidada|Devedora|Paga|A vencer|Vencida)\s+\d+'

    # Padrão 3: Formato PGFN
    # Nr.Prestação + Tipo + Valor Originário + Valor Principal + Valor Multa + Valor Juros + Valor Encargos + Valor Saldo + Data Vencimento + Situação
    # Ex: "0001 Prestaçao Básica 4.424,55 0,00 0,00 0,00 0,00 0,00 31/05/2017 Quitada"
    # Ex: "0002 Prestação 4.424,55 2.959,64 591,92 470,74 402,23 4.424,55 30/06/2017 Vencida"
    pattern3 = r'^(\d{4})\s+(?:Presta[çc][ãa]o\s*(?:B[áa]sica)?|Prestação)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+(\d{2}/\d{2}/\d{4})\s+(Quitada|Vencida|A vencer|Paga|Devedora)'

    for linha in texto.split('\n'):
        linha_strip = linha.strip()

        # Tenta padrão 3 (PGFN) primeiro - mais específico
        match = re.match(pattern3, linha_strip, re.IGNORECASE)
        if match:
            numero_parcela = int(match.group(1))
            valor_originario = parse_valor_brasileiro(match.group(2))
            valor_principal = parse_valor_brasileiro(match.group(3))
            valor_multa = parse_valor_brasileiro(match.group(4))
            valor_juros = parse_valor_brasileiro(match.group(5))
            valor_encargos = parse_valor_brasileiro(match.group(6))
            saldo_devedor = parse_valor_brasileiro(match.group(7))
            data_vencimento = match.group(8)
            situacao = match.group(9)

            parcela = {
                'numero_parcela': numero_parcela,
                'data_vencimento': parse_data_brasileira(data_vencimento),
                'valor_originario': valor_originario,
                'valor_principal': valor_principal,
                'valor_multa': valor_multa,
                'valor_juros': valor_juros,
                'valor_encargos': valor_encargos,
                'saldo_atualizado': saldo_devedor,
                'situacao': normalizar_situacao_parcela(situacao)
            }
            parcelas.append(parcela)
            continue

        # Tenta padrão 1 (e-CAC)
        match = re.match(pattern1, linha_strip, re.IGNORECASE)
        if match:
            numero_parcela = int(match.group(1))
            data_vencimento = match.group(2)
            valor_originario = parse_valor_brasileiro(match.group(3))
            saldo_atualizado = parse_valor_brasileiro(match.group(4))
            situacao = match.group(5)

            parcela = {
                'numero_parcela': numero_parcela,
                'data_vencimento': parse_data_brasileira(data_vencimento),
                'valor_originario': valor_originario,
                'saldo_atualizado': saldo_atualizado,
                'situacao': normalizar_situacao_parcela(situacao)
            }
            parcelas.append(parcela)
            continue

        # Tenta padrão 2 (PERT)
        match = re.match(pattern2, linha_strip, re.IGNORECASE)
        if match:
            numero_parcela = int(match.group(1))
            data_vencimento = match.group(2)
            valor_devido = parse_valor_brasileiro(match.group(3))
            data_pagamento = match.group(4)  # Pode ser None se for "-"
            valor_pago = parse_valor_brasileiro(match.group(5))
            saldo_devedor = parse_valor_brasileiro(match.group(6))
            situacao = match.group(7)

            parcela = {
                'numero_parcela': numero_parcela,
                'data_vencimento': parse_data_brasileira(data_vencimento),
                'valor_originario': valor_devido,
                'saldo_atualizado': saldo_devedor,
                'valor_pago': valor_pago,
                'data_pagamento': parse_data_brasileira(data_pagamento) if data_pagamento else None,
                'situacao': normalizar_situacao_parcela(situacao)
            }
            parcelas.append(parcela)

    return parcelas


def normalizar_situacao_parcela(situacao: str) -> str:
    """Normaliza a situação da parcela."""
    situacao_lower = situacao.lower()
    if 'paga' in situacao_lower or 'liquidada' in situacao_lower or 'quitada' in situacao_lower:
        return 'Paga'
    elif 'vencida' in situacao_lower:
        return 'Vencida'
    elif 'devedora' in situacao_lower:
        return 'Devedora'
    elif 'vencer' in situacao_lower:
        return 'A vencer'
    return situacao.capitalize()


def extrair_pagamentos_do_texto(texto: str) -> List[Dict]:
    """
    Extrai a lista de pagamentos diretamente do texto do PDF.
    Formato e-CAC: "23/04/2024 3.804,16 3.804,16 0,00 7032411497712762"
    """
    pagamentos = []

    # Padrão para linha de pagamento (após seção "Pagamentos"):
    # Data (dd/mm/yyyy) + Valor total + Dívida amortizada + Juros + Documento
    pattern = r'(\d{2}/\d{2}/\d{4})\s+([\d.,]+)\s+([\d.,]+)\s+([\d.,]+)\s+(\d{10,})'

    # Procura apenas após a seção "Pagamentos"
    idx_pagamentos = texto.lower().find('pagamentos')
    if idx_pagamentos > 0:
        texto_pagamentos = texto[idx_pagamentos:]

        matches = re.findall(pattern, texto_pagamentos)

        for match in matches:
            data_pagamento = match[0]
            valor_total = parse_valor_brasileiro(match[1])
            divida_amortizada = parse_valor_brasileiro(match[2])
            juros_amortizados = parse_valor_brasileiro(match[3])
            documento = match[4]

            pagamento = {
                'data_pagamento': parse_data_brasileira(data_pagamento),
                'valor_pago': valor_total,
                'divida_amortizada': divida_amortizada,
                'juros_amortizados': juros_amortizados,
                'darf_numero': documento
            }
            pagamentos.append(pagamento)

    return pagamentos


def extrair_dados_parcelamento(texto: str) -> Dict:
    """Extrai os dados gerais do parcelamento do texto."""
    dados = {
        'numero_parcelamento': None,
        'cnpj': None,
        'orgao': None,
        'modalidade': None,
        'situacao': None,
        'data_inicio': None,
        'data_adesao': None,
        'data_consolidacao': None,
        'data_encerramento': None,
        'motivo_encerramento': None,
        'qtd_parcelas': None,
        'valor_parcela': None
    }

    # Padrões de regex para extração
    # Formato e-CAC RFB: "Parcelamento: 0211.00012.0054927466.24-20"
    # Formato situação: "Situação do parcelamento: Parcelamento rescindido"
    # Formato data exclusão: "Data de efeito da exclusão: 08/04/2025"
    # Formato motivo: "Motivo da exclusão: Inadimplência de parcelas"
    patterns = {
        'numero_parcelamento': [
            # Formato e-CAC RFB: 0211.00012.0054927466.24-20
            r'Parcelamento[:\s]*(\d{4}\.\d{5}\.\d{10}\.\d{2}-\d{2})',
            r'Parcelamento[:\s]*(\d{4}\.\d{5}\.\d+\.\d{2}-\d{2})',
            r'Parcelamento[:\s]*(\d+\.\d+\.\d+\.\d+-\d+)',
            # Formato PERT: número na linha seguinte ao label
            r'N[º°]\s*do\s*Parcelamento\s+Saldo\s+Devedor[^\n]*\n(\d{9,})',
            r'N[º°]\s*do\s*Parcelamento[^\n]*\n(\d{9,})',
            # Formato PGFN scrambled: "N N º eg m o e c r ia o ç ã a o: 1152177" ou "10063354"
            r'o[:\s]*(\d{7,8})\s+N\s*C?\s*o',  # Número seguido de "N" ou "Nome"
            r'[Nn]\s*[Nn]\s*[º°].*?[oO][:\s]*(\d{7,8})',
            # Formato PGFN: número no início da linha seguido de nome
            r'\n(\d{7,8})\s+[A-Z]+\s+Negocia',
            r'N[úu]mero da Negocia[çc][ãa]o[:\s]*(\d{7,8})',
            r'N[úu]mero Negocia[çc][ãa]o[:\s]*(\d{7,8})',
            r'N[úu]mero da\s+(\d{7,8})\s+Nome',  # "Número da 4285230 Nome" (sem scrambling)
            r'[Nn]egocia[çc][ãa]o[:\s]*(\d{7,8})',
            r'Número do Parcelamento[:\s]*(\d+)',
            r'Parcelamento[:\s]*[Nn]º?\s*(\d+)',
            r'Nº do Parcelamento[:\s]*(\d+)',
            r'N[úu]mero[:\s]*(\d{9,})'
        ],
        'cnpj': [
            r'CNPJ[:\s]*(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})',
            r'(\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2})'
        ],
        'situacao': [
            # Formato e-CAC RFB: "Situação do parcelamento: Parcelamento rescindido"
            r'Situa[çc][ãa]o do parcelamento[:\s]*[Pp]arcelamento\s+(rescindido|ativo|quitado|liquidado|consolidado|suspenso|cancelado)',
            r'Situa[çc][ãa]o do parcelamento[:\s]*(Ativo|Ativa|Rescindido|Rescindida|Quitado|Quitada|Liquidado|Consolidado)',
            # Formato PERT: situação na linha seguinte ao label
            # "Situação do Parcelamento Quantidade de Parcelas restantes"
            # "ATIVO (EM DIA) 48"
            r'Situa[çc][ãa]o do Parcelamento\s+Quantidade[^\n]*\n(ATIVO|ATIVA|RESCINDIDO|RESCINDIDA|QUITADO|QUITADA|LIQUIDADO)',
            r'Situa[çc][ãa]o do Parcelamento[^\n]*\n(ATIVO|ATIVA|RESCINDIDO|RESCINDIDA|QUITADO|QUITADA|LIQUIDADO)',
            # Formato PGFN scrambled: várias formas de "ENCERRADA POR RESCISAO"
            r'Situa[çc][ãa]o[:\s]*E\s*P?\s*N?\s*O?\s*C?\s*R?\s*E?\s*R?\s*R?\s*E?\s*R?\s*S?\s*A?\s*C?\s*D?\s*I?\s*A?\s*(SAO)',
            r'Situa[çc][ãa]o[:\s]*E\s*R\s*N?\s*E?\s*S?\s*C?\s*C?\s*E?\s*R?\s*I?\s*S?\s*R?\s*A?\s*A?\s*O?\s*D?\s*A?\s*(POR)',
            r'Situa[çc][ãa]o[:\s]*E\s*R\s*N\s*E\s*S\s*C\s*C\s*E\s*I\s*R\s*S\s*R\s*A\s*A\s*O\s*D\s*A\s*(POR)',  # Outro scrambled
            r'Situa[çc][ãa]o[:\s]*(POR)\s+Principal',  # "Situação: POR Principal" - PGFN scrambled
            r'Situa[çc][ãa]o[:\s]*POR\s+(RESCISAO)',  # "Situação: POR RESCISAO"
            r'Situa[çc][ãa]o[:\s]*E\s*IN\s*N?\s*D?\s*C?\s*E?\s*E?\s*F?\s*R?\s*E?\s*R?\s*R?\s*A?\s*I?\s*D?\s*M?\s*A?\s*E?\s*N?\s*PO\s*(TO)',  # ENCERRADA INDEFERIMENTO POR
            r'Situa[çc][ãa]o[:\s]*(ENCERRAD[AO]\s+POR\s+RESCIS[ÃA]O)',
            r'Situa[çc][ãa]o[:\s]*(ATIV[AO]|ENCERRAD[AO]|QUITAD[AO]|LIQUIDAD[AO]|CONSOLIDAD[AO]|RESCINDID[AO])',
            r'Situa[çc][ãa]o[:\s]*(Ativo|Ativa|Inativo|Inativa|Rescindido|Rescindida|Consolidado|Consolidada|Em análise|Quitado|Quitada|Liquidado|Liquidada|Suspenso|Suspensa|Cancelado|Cancelada|Encerrad[ao])',
            r'Status[:\s]*(Ativo|Ativa|Inativo|Inativa|Rescindido|Rescindida|Consolidado|Consolidada|Em análise|Quitado|Quitada|Liquidado|Liquidada)',
            r'SITUA[ÇC][ÃA]O[:\s]*(ATIVO|ATIVA|INATIVO|INATIVA|RESCINDIDO|RESCINDIDA|CONSOLIDADO|CONSOLIDADA|QUITADO|QUITADA|ENCERRAD[AO])'
        ],
        'data_inicio': [
            # Formato PERT: "Data da Negociação Quantidade de Parcelas concedidas"
            #               "31/10/2017 145"
            r'Data da Negocia[çc][ãa]o\s+Quantidade[^\n]*\n(\d{2}/\d{2}/\d{4})',
            r'Data da Negocia[çc][ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data de In[íi]cio[:\s]*(\d{2}/\d{2}/\d{4})',
            r'In[íi]cio do Parcelamento[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data In[íi]cio[:\s]*(\d{2}/\d{2}/\d{4})',
            r'In[íi]cio[:\s]*(\d{2}/\d{2}/\d{4})'
        ],
        'data_adesao': [
            # Formato PERT: "Data da Negociação" = data de adesão
            r'Data da Negocia[çc][ãa]o\s+Quantidade[^\n]*\n(\d{2}/\d{2}/\d{4})',
            r'Data da Negocia[çc][ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            # Formato PGFN scrambled: "Data da Adesão: 2 1 9 0 / :2 0 4 5/2017" -> usa .o para aceitar ã codificado
            r'Ades.o[:\s]*.*?(\d{2}/\d{2}/\d{4})',  # Captura qualquer data após "Adesão" (aceita encoding)
            r'Data da Ades[ãa]o[:\s]*.*?(\d{2}/\d{2}/\d{4})',
            r'Data de Ades[ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Ades[ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Ades[ãa]o em[:\s]*(\d{2}/\d{2}/\d{4})'
        ],
        'data_consolidacao': [
            # Formato PGFN scrambled: usa .+ para aceitar texto scrambled entre "Consolida" e a data
            r'[Cc]onsolida.+?[:\s]*(\d{2}/\d{2}/\d{4})',
            r'[Cc]onsolida[çc][ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data da Consolida[çc][ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data de Consolida[çc][ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data da consolida[çc][ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Consolidado em[:\s]*(\d{2}/\d{2}/\d{4})'
        ],
        'data_encerramento': [
            # Formato e-CAC RFB: "Data de efeito da exclusão: 08/04/2025"
            r'Data de efeito da exclus[ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data da exclus[ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data de solicita[çc][ãa]o de desist[êe]ncia[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data de Encerramento[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Encerramento[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data do Encerramento[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Encerrado em[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data de Rescis[ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Rescindido em[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data da Rescis[ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data de Quita[çc][ãa]o[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Quitado em[:\s]*(\d{2}/\d{2}/\d{4})',
            r'Data Liquida[çc][ãa]o[:\s]*(\d{2}/\d{2}/\d{4})'
        ],
        'motivo_encerramento': [
            # Formato e-CAC RFB: "Motivo da exclusão: Inadimplência de parcelas"
            r'Motivo da exclus[ãa]o[:\s]*([^\n]+)',
            r'Motivo do Encerramento[:\s]*([^\n]+)',
            r'Motivo da Rescis[ãa]o[:\s]*([^\n]+)',
            r'Motivo[:\s]*(Inadimpl[êe]ncia[^\n]*|Quita[çc][ãa]o|Liquida[çc][ãa]o|Pagamento integral|Falta de pagamento|Desist[êe]ncia)',
            r'Causa do Encerramento[:\s]*([^\n]+)'
        ],
        'qtd_parcelas': [
            # Formato PERT: quantidade na linha seguinte ao label
            # "Data da Negociação Quantidade de Parcelas concedidas"
            # "31/10/2017 145"
            r'Quantidade de Parcelas concedidas\n\d{2}/\d{2}/\d{4}\s+(\d+)',
            r'Quantidade de Parcelas concedidas\s+(\d+)',
            r'Quantidade de Parcelas[:\s]*(\d+)',
            # Formato PGFN: "Quantidade de Prestações: 120"
            r'Quantidade de Presta[çc][õo]es[:\s]*(\d+)',
            r'N[úu]mero de Parcelas[:\s]*(\d+)',
            r'Total de parcelas\s+.*?\n(\d+)\s+\d+',  # Formato e-CAC: "Total de parcelas" na linha e número na próxima
            r'Qtd\.?\s*Parcelas[:\s]*(\d+)'
            # Removido: r'(\d+)\s*parcelas' - muito genérico, pegava ano de datas
        ],
        'valor_parcela': [
            r'Valor da Parcela[:\s]*R\$\s*([\d.,]+)',
            r'Parcela[:\s]*R\$\s*([\d.,]+)',
            r'Valor Parcela[:\s]*R\$\s*([\d.,]+)',
            r'Presta[çc][ãa]o[:\s]*R\$\s*([\d.,]+)'
        ]
    }

    for campo, lista_patterns in patterns.items():
        for pattern in lista_patterns:
            match = re.search(pattern, texto, re.IGNORECASE)
            if match:
                valor = match.group(1).strip()
                if campo in ['data_inicio', 'data_adesao', 'data_consolidacao', 'data_encerramento']:
                    dados[campo] = parse_data_brasileira(valor)
                elif campo == 'qtd_parcelas':
                    try:
                        dados[campo] = int(valor)
                    except ValueError:
                        pass
                elif campo == 'valor_parcela':
                    dados[campo] = parse_valor_brasileiro(valor)
                elif campo == 'situacao':
                    # Normaliza situação
                    situacao_lower = valor.lower()
                    if 'ativo' in situacao_lower or 'ativa' in situacao_lower:
                        dados[campo] = 'Ativo'
                    elif 'encerrad' in situacao_lower and 'rescis' in situacao_lower:
                        # PGFN: "ENCERRADA POR RESCISAO"
                        dados[campo] = 'Rescindido'
                    elif situacao_lower in ('sao', 'por', 'rescisao', 'to'):
                        # PGFN scrambled: capturou "SAO" de "RESCISAO", "POR" de "POR RESCISAO", ou "TO" de "INDEFERIMENTO"
                        dados[campo] = 'Rescindido'
                    elif 'rescindid' in situacao_lower or 'rescis' in situacao_lower:
                        dados[campo] = 'Rescindido'
                    elif 'encerrad' in situacao_lower:
                        dados[campo] = 'Rescindido'  # PGFN: "ENCERRADA" = Rescindido
                    elif 'quitad' in situacao_lower or 'liquidado' in situacao_lower or 'liquidada' in situacao_lower:
                        dados[campo] = 'Quitado'
                    elif 'consolidado' in situacao_lower or 'consolidada' in situacao_lower:
                        dados[campo] = 'Consolidado'
                    elif 'inativo' in situacao_lower or 'inativa' in situacao_lower:
                        dados[campo] = 'Inativo'
                    elif 'suspens' in situacao_lower:
                        dados[campo] = 'Suspenso'
                    elif 'cancelad' in situacao_lower:
                        dados[campo] = 'Cancelado'
                    else:
                        dados[campo] = valor.capitalize()
                else:
                    dados[campo] = valor
                break

    # Se não encontrou data_inicio, usa data_adesao ou data_consolidacao como fallback
    if not dados['data_inicio']:
        if dados['data_adesao']:
            dados['data_inicio'] = dados['data_adesao']
        elif dados['data_consolidacao']:
            dados['data_inicio'] = dados['data_consolidacao']

    # Identifica órgão (Receita Federal, PGFN, Procuradoria)
    texto_lower = texto.lower()
    if 'procuradoria' in texto_lower or 'pgfn' in texto_lower and 'procuradoria' in texto_lower:
        dados['orgao'] = 'Procuradoria'
    elif 'pgfn' in texto_lower:
        dados['orgao'] = 'PGFN'
    elif 'receita federal' in texto_lower or 'rfb' in texto_lower:
        dados['orgao'] = 'Receita Federal'
    else:
        dados['orgao'] = 'Receita Federal'

    # Identifica modalidade
    modalidades = [
        ('PERT', ['pert', 'programa especial de regularização tributária']),
        ('REFIS', ['refis']),
        ('PAES', ['paes']),
        ('PAEX', ['paex']),
        ('LEI 11.941', ['lei 11.941', '11941', 'lei 11941']),
        ('LEI 12.996', ['lei 12.996', '12996', 'lei 12996']),
        ('LEI 13.496', ['lei 13.496', '13496', 'lei 13496']),
        ('SIMPLES NACIONAL', ['simples nacional', 'simples']),
        ('PARCELAMENTO ORDINÁRIO', ['parcelamento ordinário', 'ordinario', 'parcelamento comum']),
        ('PARCELAMENTO SIMPLIFICADO', ['simplificado']),
        ('TRANSAÇÃO', ['transação', 'transacao'])
    ]

    for mod_nome, keywords in modalidades:
        for keyword in keywords:
            if keyword in texto_lower:
                dados['modalidade'] = mod_nome
                break
        if dados['modalidade']:
            break

    return dados


def extrair_resumo_divida(texto: str) -> Dict:
    """Extrai o resumo da dívida consolidada do texto."""
    resumo = {
        'valor_total_consolidado': 0.0,
        'valor_principal': 0.0,
        'valor_multa': 0.0,
        'valor_juros': 0.0,
        'saldo_devedor': 0.0
    }

    # Formato e-CAC RFB:
    # "Dívida consolidada no parcelamento (BRL) 228.249,60"
    # "Principal (BRL) 190.208,04 190.208,04" (segundo valor = consolidado)
    # "Multa (BRL) 38.041,56 38.041,56"
    # "Total (BRL) 228.249,60 228.249,60"
    # "Saldo devedor em 21/01/2026 (BRL) 223.496,30"
    # Formato PGFN: "Principal: 591.929,41" | "Multa: 118.385,89" | "Juros: 94.149,02" | "Valor Consolidado: 884.910,56"
    patterns = {
        'valor_total_consolidado': [
            r'D[íi]vida consolidada no parcelamento \(BRL\)\s*([\d.,]+)',
            r'Total \(BRL\)\s*[\d.,]+\s+([\d.,]+)',  # Pega segundo valor (consolidado)
            # Formato PGFN: "Valor Consolidado: 884.910,56"
            r'Valor Consolidado[:\s]*([\d.,]+)',
            r'Valor Total Consolidado[:\s]*R?\$?\s*([\d.,]+)',
            r'Total Consolidado[:\s]*R?\$?\s*([\d.,]+)',
            r'D[íi]vida Consolidada[:\s]*R?\$?\s*([\d.,]+)'
        ],
        'valor_principal': [
            r'Principal \(BRL\)\s*[\d.,]+\s+([\d.,]+)',  # Pega segundo valor (consolidado)
            r'Principal \(BRL\)\s*([\d.,]+)',
            # Formato PGFN: "Principal: 591.929,41"
            r'Principal[:\s]*([\d.,]+)',
            r'Valor Principal[:\s]*R?\$?\s*([\d.,]+)'
        ],
        'valor_multa': [
            r'Multa \(BRL\)\s*[\d.,]+\s+([\d.,]+)',  # Pega segundo valor (consolidado)
            r'Multa \(BRL\)\s*([\d.,]+)',
            # Formato PGFN: "Multa: 118.385,89"
            r'Multa[:\s]*([\d.,]+)',
            r'Valor Multa[:\s]*R?\$?\s*([\d.,]+)'
        ],
        'valor_juros': [
            r'Juros \(BRL\)\s*[\d.,]+\s+([\d.,]+)',  # Pega segundo valor (consolidado)
            r'Juros \(BRL\)\s*([\d.,]+)',
            # Formato PGFN: "Juros: 94.149,02"
            r'Juros[:\s]*([\d.,]+)',
            r'Valor Juros[:\s]*R?\$?\s*([\d.,]+)'
        ],
        'saldo_devedor': [
            # Formato PERT: saldo na linha seguinte ao label
            # "Nº do Parcelamento Saldo Devedor do Parcelamento"
            # "625509536 R$ 648.872,71"
            r'Saldo Devedor do Parcelamento\n\d+\s+R\$\s*([\d.,]+)',
            r'Saldo Devedor do Parcelamento\s+R\$\s*([\d.,]+)',
            r'Saldo devedor em \d{2}/\d{2}/\d{4} \(BRL\)\s*([\d.,]+)',
            # Formato PGFN scrambled: "S co a m ld o J u D r e o v s: edor 1.521.391,77"
            r'[Ss]\s*[co]\s*[ao]\s*[mn]?\s*[lo]\s*[do]?\s*[J]?\s*[ou]\s*[Dr]?\s*[eo]\s*[sv]?\s*[:\s]*edor\s+([\d.,]+)',
            r'edor\s+([\d.,]+)',  # Captura valor após "edor"
            # Formato PGFN: "Saldo Devedor com Juros: 1.521.391,77"
            r'Saldo Devedor com Juros[:\s]*([\d.,]+)',
            r'Saldo Devedor sem Juros[:\s]*([\d.,]+)',
            r'Saldo Devedor[:\s]*R?\$?\s*([\d.,]+)',
            r'Saldo Atual[:\s]*R?\$?\s*([\d.,]+)'
        ]
    }

    for campo, lista_patterns in patterns.items():
        for pattern in lista_patterns:
            match = re.search(pattern, texto, re.IGNORECASE)
            if match:
                resumo[campo] = parse_valor_brasileiro(match.group(1))
                break

    return resumo


def processar_tabela_debitos(tabela: List[List[str]]) -> List[Dict]:
    """Processa a tabela de débitos do parcelamento."""
    debitos = []

    # Pula o cabeçalho
    for linha in tabela[1:]:
        if not linha or len(linha) < 4:
            continue

        # Tenta extrair as colunas (pode variar dependendo do formato)
        try:
            debito = {}

            # Limpa valores nulos
            linha_limpa = [str(c).strip() if c else '' for c in linha]

            # Tenta identificar as colunas
            for i, valor in enumerate(linha_limpa):
                # Código da receita (geralmente 4 dígitos)
                if re.match(r'^\d{4}$', valor) and 'codigo_receita' not in debito:
                    debito['codigo_receita'] = valor

                # Período de apuração (formato mm/yyyy ou dd/mm/yyyy)
                elif re.match(r'^\d{2}/\d{4}$', valor) and 'periodo_apuracao' not in debito:
                    debito['periodo_apuracao'] = valor
                elif re.match(r'^\d{2}/\d{2}/\d{4}$', valor):
                    if 'periodo_apuracao' not in debito:
                        debito['periodo_apuracao'] = valor
                    elif 'data_vencimento' not in debito:
                        debito['data_vencimento'] = parse_data_brasileira(valor)

                # Valores monetários
                elif re.match(r'^[\d.,]+$', valor) and len(valor) > 2:
                    valor_num = parse_valor_brasileiro(valor)
                    if valor_num > 0:
                        if 'valor_principal' not in debito:
                            debito['valor_principal'] = valor_num
                        elif 'valor_multa' not in debito:
                            debito['valor_multa'] = valor_num
                        elif 'valor_juros' not in debito:
                            debito['valor_juros'] = valor_num
                        elif 'valor_total' not in debito:
                            debito['valor_total'] = valor_num

            # Calcula total se não foi extraído
            if 'valor_total' not in debito:
                debito['valor_total'] = debito.get('valor_principal', 0) + \
                                        debito.get('valor_multa', 0) + \
                                        debito.get('valor_juros', 0)

            # Só adiciona se tiver pelo menos código da receita ou valor
            if debito.get('codigo_receita') or debito.get('valor_principal'):
                debitos.append(debito)

        except Exception:
            continue

    return debitos


def processar_tabela_parcelas(tabela: List[List[str]]) -> List[Dict]:
    """Processa a tabela de parcelas do parcelamento."""
    parcelas = []

    # Identifica colunas pelo cabeçalho
    cabecalho = [str(c).lower().strip() if c else '' for c in tabela[0]]

    # Mapeamento de colunas possíveis
    col_map = {
        'numero_parcela': ['parcela', 'nº', 'numero', 'n°'],
        'data_vencimento': ['vencimento', 'data vencimento', 'venc.'],
        'valor_originario': ['valor originário', 'valor original', 'valor orig.', 'originário'],
        'saldo_atualizado': ['saldo atualizado', 'saldo atual', 'saldo'],
        'situacao': ['situação', 'status', 'sit.']
    }

    # Encontra índices das colunas
    indices = {}
    for campo, aliases in col_map.items():
        for i, col in enumerate(cabecalho):
            for alias in aliases:
                if alias in col:
                    indices[campo] = i
                    break
            if campo in indices:
                break

    # Processa cada linha
    for linha in tabela[1:]:
        if not linha or all(not c for c in linha):
            continue

        linha_limpa = [str(c).strip() if c else '' for c in linha]

        parcela = {}

        for campo, idx in indices.items():
            if idx < len(linha_limpa):
                valor = linha_limpa[idx]

                if campo == 'numero_parcela':
                    try:
                        parcela[campo] = int(re.sub(r'\D', '', valor)) if valor else None
                    except ValueError:
                        parcela[campo] = None

                elif campo == 'data_vencimento':
                    parcela[campo] = parse_data_brasileira(valor)

                elif campo in ['valor_originario', 'saldo_atualizado']:
                    parcela[campo] = parse_valor_brasileiro(valor)

                elif campo == 'situacao':
                    # Normaliza a situação
                    situacao_lower = valor.lower()
                    if 'paga' in situacao_lower:
                        parcela[campo] = 'Paga'
                    elif 'devedora' in situacao_lower or 'vencida' in situacao_lower:
                        parcela[campo] = 'Devedora'
                    elif 'vencer' in situacao_lower:
                        parcela[campo] = 'A vencer'
                    else:
                        parcela[campo] = valor

        # Só adiciona se tiver número da parcela ou vencimento
        if parcela.get('numero_parcela') or parcela.get('data_vencimento'):
            parcelas.append(parcela)

    return parcelas


def processar_tabela_pagamentos(tabela: List[List[str]]) -> List[Dict]:
    """Processa a tabela de pagamentos realizados."""
    pagamentos = []

    for linha in tabela[1:]:
        if not linha or all(not c for c in linha):
            continue

        linha_limpa = [str(c).strip() if c else '' for c in linha]

        pagamento = {}

        for valor in linha_limpa:
            # Data de pagamento
            if re.match(r'^\d{2}/\d{2}/\d{4}$', valor) and 'data_pagamento' not in pagamento:
                pagamento['data_pagamento'] = parse_data_brasileira(valor)

            # Valor pago
            elif re.match(r'^[\d.,]+$', valor) and len(valor) > 2:
                valor_num = parse_valor_brasileiro(valor)
                if valor_num > 0 and 'valor_pago' not in pagamento:
                    pagamento['valor_pago'] = valor_num

            # Número DARF (geralmente sequência numérica longa)
            elif re.match(r'^\d{10,}$', valor) and 'darf_numero' not in pagamento:
                pagamento['darf_numero'] = valor

        if pagamento.get('data_pagamento') or pagamento.get('valor_pago'):
            pagamentos.append(pagamento)

    return pagamentos


def gerar_lancamentos_parcelamento(
    parcelamento: Dict,
    valor_pago: float,
    data_pagamento: str,
    proporcao_principal: float = None,
    proporcao_multa: float = None,
    proporcao_juros: float = None
) -> List[Dict]:
    """
    Gera os lançamentos contábeis para um pagamento de parcelamento.
    Separa em Principal, Multa e Juros.

    Args:
        parcelamento: Dados do parcelamento (com contas contábeis configuradas)
        valor_pago: Valor total pago
        data_pagamento: Data do pagamento (formato YYYY-MM-DD)
        proporcao_principal: Proporção do principal (se None, calcula automaticamente)
        proporcao_multa: Proporção da multa
        proporcao_juros: Proporção dos juros

    Returns:
        Lista de dicionários com os lançamentos contábeis
    """
    lancamentos = []

    # Se proporções não foram informadas, calcula com base nos valores do parcelamento
    if proporcao_principal is None:
        valor_total = (parcelamento.get('valor_principal', 0) +
                       parcelamento.get('valor_multa', 0) +
                       parcelamento.get('valor_juros', 0))

        if valor_total > 0:
            proporcao_principal = parcelamento.get('valor_principal', 0) / valor_total
            proporcao_multa = parcelamento.get('valor_multa', 0) / valor_total
            proporcao_juros = parcelamento.get('valor_juros', 0) / valor_total
        else:
            # Se não tem valores, assume 100% principal
            proporcao_principal = 1.0
            proporcao_multa = 0.0
            proporcao_juros = 0.0

    # Calcula valores
    valor_principal = round(valor_pago * proporcao_principal, 2)
    valor_multa = round(valor_pago * proporcao_multa, 2)
    valor_juros = round(valor_pago * (proporcao_juros or 0), 2)

    # Ajusta diferença de arredondamento
    diferenca = valor_pago - (valor_principal + valor_multa + valor_juros)
    valor_principal += diferenca

    # Gera ID único para o lançamento
    id_lancamento = f"PARC-{parcelamento.get('numero_parcelamento', 'X')}-{data_pagamento.replace('-', '')}"

    historico_base = f"Pgto Parcelamento {parcelamento.get('numero_parcelamento', '')} - {parcelamento.get('orgao', 'RFB')}"

    # Lançamento do Principal (D - Tributos a Pagar / C - Banco)
    if valor_principal > 0:
        lancamentos.append({
            'idlancamento': f"{id_lancamento}-P",
            'data_lancamento': data_pagamento,
            'historico': f"{historico_base} - Principal",
            'valor': valor_principal,
            'tipo_lancamento': 'Simples',
            'reduz_deb': parcelamento.get('conta_contabil_principal', ''),
            'nome_conta_d': 'Tributos a Pagar - Principal',
            'reduz_cred': parcelamento.get('conta_contabil_banco', ''),
            'nome_conta_c': 'Banco',
            'origem': 'Parcelamento'
        })

    # Lançamento da Multa (D - Despesa Multas / C - Banco)
    if valor_multa > 0:
        lancamentos.append({
            'idlancamento': f"{id_lancamento}-M",
            'data_lancamento': data_pagamento,
            'historico': f"{historico_base} - Multa",
            'valor': valor_multa,
            'tipo_lancamento': 'Simples',
            'reduz_deb': parcelamento.get('conta_contabil_multa', ''),
            'nome_conta_d': 'Despesas com Multas Tributárias',
            'reduz_cred': parcelamento.get('conta_contabil_banco', ''),
            'nome_conta_c': 'Banco',
            'origem': 'Parcelamento'
        })

    # Lançamento dos Juros (D - Despesa Juros / C - Banco)
    if valor_juros > 0:
        lancamentos.append({
            'idlancamento': f"{id_lancamento}-J",
            'data_lancamento': data_pagamento,
            'historico': f"{historico_base} - Juros",
            'valor': valor_juros,
            'tipo_lancamento': 'Simples',
            'reduz_deb': parcelamento.get('conta_contabil_juros', ''),
            'nome_conta_d': 'Despesas com Juros Tributários',
            'reduz_cred': parcelamento.get('conta_contabil_banco', ''),
            'nome_conta_c': 'Banco',
            'origem': 'Parcelamento'
        })

    return lancamentos


def conciliar_parcela_extrato(
    parcelas_pendentes: pd.DataFrame,
    transacoes_extrato: pd.DataFrame,
    tolerancia_valor: float = 0.01,
    tolerancia_dias: int = 5
) -> List[Dict]:
    """
    Concilia parcelas pendentes com transações do extrato bancário.

    Args:
        parcelas_pendentes: DataFrame com parcelas pendentes de conciliação
        transacoes_extrato: DataFrame com transações do extrato bancário
        tolerancia_valor: Tolerância percentual para comparação de valores
        tolerancia_dias: Tolerância em dias para comparação de datas

    Returns:
        Lista de dicionários com as conciliações encontradas
    """
    conciliacoes = []

    for _, parcela in parcelas_pendentes.iterrows():
        valor_parcela = parcela.get('saldo_atualizado') or parcela.get('valor_originario', 0)
        data_vencimento = pd.to_datetime(parcela.get('data_vencimento'))

        if pd.isna(valor_parcela) or valor_parcela == 0:
            continue

        # Procura transações compatíveis
        for _, transacao in transacoes_extrato.iterrows():
            valor_transacao = abs(transacao.get('Valor', 0))
            data_transacao = pd.to_datetime(transacao.get('Data Lançamento'))

            # Verifica se é débito (valor negativo no extrato)
            if transacao.get('Valor', 0) >= 0:
                continue

            # Verifica valor dentro da tolerância
            diff_valor = abs(valor_parcela - valor_transacao) / valor_parcela if valor_parcela > 0 else 1
            if diff_valor > tolerancia_valor:
                continue

            # Verifica data dentro da tolerância
            if pd.notna(data_vencimento) and pd.notna(data_transacao):
                diff_dias = abs((data_transacao - data_vencimento).days)
                if diff_dias > tolerancia_dias:
                    continue

            # Encontrou conciliação
            conciliacoes.append({
                'parcela_id': parcela.get('id'),
                'numero_parcela': parcela.get('numero_parcela'),
                'valor_parcela': valor_parcela,
                'data_vencimento': parcela.get('data_vencimento'),
                'id_transacao': transacao.get('ID Transacao'),
                'valor_transacao': valor_transacao,
                'data_transacao': transacao.get('Data Lançamento'),
                'descricao_transacao': transacao.get('Descrição', ''),
                'score': 1 - diff_valor  # Quanto mais próximo de 1, melhor
            })
            break  # Só uma conciliação por parcela

    return conciliacoes
