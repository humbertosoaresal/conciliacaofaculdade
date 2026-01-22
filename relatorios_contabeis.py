# relatorios_contabeis.py
import pandas as pd
import streamlit as st
from datetime import date, datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.pdfgen import canvas
from io import BytesIO
import os

# Paleta de cores moderna
COR_PRINCIPAL = colors.HexColor('#1e3a8a')  # Azul escuro profissional
COR_SECUNDARIA = colors.HexColor('#3b82f6')  # Azul médio
COR_ACENTO = colors.HexColor('#60a5fa')  # Azul claro
COR_FUNDO_HEADER = colors.HexColor('#1e40af')  # Azul para cabeçalho de tabela
COR_FUNDO_ZEBRA = colors.HexColor('#f0f9ff')  # Azul muito claro para linhas alternadas
COR_TEXTO_HEADER = colors.whitesmoke
COR_BORDA = colors.HexColor('#cbd5e1')  # Cinza claro para bordas
COR_TOTAL = colors.HexColor('#e0f2fe')  # Azul muito claro para linha de totais


class NumberedCanvas(canvas.Canvas):
    """Canvas customizado para adicionar rodapé com numeração de páginas."""
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_number(num_pages)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)

    def draw_page_number(self, page_count):
        """Desenha rodapé com número da página e data de geração."""
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor('#64748b'))

        # Número da página (centro)
        page_num_text = f"Página {self._pageNumber} de {page_count}"
        self.drawCentredString(A4[0] / 2, 15*mm, page_num_text)

        # Data de geração (direita)
        data_geracao = datetime.now().strftime('%d/%m/%Y às %H:%M')
        self.drawRightString(A4[0] - 15*mm, 15*mm, f"Gerado em: {data_geracao}")

        # Linha decorativa
        self.setStrokeColor(COR_BORDA)
        self.setLineWidth(0.5)
        self.line(15*mm, 18*mm, A4[0] - 15*mm, 18*mm)

def criar_cabecalho_relatorio(elements, empresa_info, logo_path, titulo_relatorio, periodo_str=""):
    """Cria um cabeçalho moderno e profissional para os relatórios."""
    styles = getSampleStyleSheet()

    # Criar uma tabela de cabeçalho com fundo colorido
    header_data = []

    # Linha 1: Logo (se existir) e Dados da empresa
    if logo_path and os.path.exists(logo_path):
        logo = Image(logo_path, width=40*mm, height=15*mm, kind='proportional')

        # Dados da empresa em HTML para melhor formatação
        empresa_html = f"""
        <b><font size=12 color='#1e3a8a'>{empresa_info.get('razao_social', 'EMPRESA')}</font></b><br/>
        """

        cnpj = empresa_info.get('cnpj', '')
        if cnpj and len(cnpj) == 14:
            cnpj_formatado = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
            empresa_html += f"<font size=9 color='#475569'>CNPJ: {cnpj_formatado}</font><br/>"

        if empresa_info.get('logradouro'):
            endereco = f"{empresa_info['logradouro']}, {empresa_info.get('numero', 's/n')}"
            if empresa_info.get('bairro'):
                endereco += f" - {empresa_info['bairro']}"
            if empresa_info.get('municipio') and empresa_info.get('uf'):
                endereco += f" - {empresa_info['municipio']}/{empresa_info['uf']}"
            empresa_html += f"<font size=8 color='#64748b'>{endereco}</font>"

        style_empresa = ParagraphStyle('EmpresaModerno', parent=styles['Normal'], alignment=TA_LEFT)

        header_data.append([logo, Paragraph(empresa_html, style_empresa)])
        header_table = Table(header_data, colWidths=[45*mm, 135*mm])
        header_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (0, 0), 'LEFT'),
            ('ALIGN', (1, 0), (1, 0), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ]))
        elements.append(header_table)
    else:
        # Sem logo, apenas dados da empresa
        style_empresa = ParagraphStyle(
            'EmpresaSemLogo',
            parent=styles['Normal'],
            fontSize=12,
            textColor=COR_PRINCIPAL,
            fontName='Helvetica-Bold',
            alignment=TA_CENTER,
            spaceAfter=2
        )
        elements.append(Paragraph(empresa_info.get('razao_social', 'EMPRESA'), style_empresa))

        cnpj = empresa_info.get('cnpj', '')
        if cnpj and len(cnpj) == 14:
            cnpj_formatado = f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"
            style_cnpj = ParagraphStyle('CNPJ', parent=styles['Normal'], fontSize=9,
                                       textColor=colors.HexColor('#475569'), alignment=TA_CENTER)
            elements.append(Paragraph(f"CNPJ: {cnpj_formatado}", style_cnpj))

    elements.append(Spacer(1, 8*mm))

    # Linha decorativa
    linha_decorativa = Table([['']], colWidths=[180*mm], rowHeights=[1])
    linha_decorativa.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), COR_SECUNDARIA),
        ('LINEABOVE', (0, 0), (-1, -1), 2, COR_SECUNDARIA),
    ]))
    elements.append(linha_decorativa)
    elements.append(Spacer(1, 5*mm))

    # Título do relatório com fundo colorido
    style_titulo = ParagraphStyle(
        'TituloModerno',
        parent=styles['Normal'],
        fontSize=16,
        textColor=colors.white,
        fontName='Helvetica-Bold',
        alignment=TA_CENTER,
        leading=20
    )

    titulo_table = Table([[Paragraph(titulo_relatorio, style_titulo)]], colWidths=[180*mm])
    titulo_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), COR_FUNDO_HEADER),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('ROUNDEDCORNERS', [5, 5, 5, 5]),
    ]))
    elements.append(titulo_table)

    if periodo_str:
        elements.append(Spacer(1, 3*mm))
        style_periodo = ParagraphStyle(
            'PeriodoModerno',
            parent=styles['Normal'],
            fontSize=10,
            textColor=colors.HexColor('#475569'),
            alignment=TA_CENTER,
            fontName='Helvetica-Bold'
        )
        elements.append(Paragraph(periodo_str, style_periodo))

    elements.append(Spacer(1, 8*mm))

    return elements


def gerar_balancete_pdf(df_lancamentos: pd.DataFrame, df_plano_contas: pd.DataFrame,
                        empresa_info: dict, logo_path: str, data_inicio: date, data_fim: date) -> BytesIO:
    """
    Gera PDF do Balancete de Verificação com design moderno.
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                           topMargin=15*mm, bottomMargin=25*mm,
                           leftMargin=15*mm, rightMargin=15*mm)

    elements = []
    periodo_str = f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}"

    criar_cabecalho_relatorio(elements, empresa_info, logo_path,
                              "BALANCETE DE VERIFICAÇÃO", periodo_str)

    # Converter data_lancamento para datetime se necessário
    if not pd.api.types.is_datetime64_any_dtype(df_lancamentos['data_lancamento']):
        df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'])

    # Filtrar lançamentos do período
    df_periodo = df_lancamentos[
        (df_lancamentos['data_lancamento'].dt.date >= data_inicio) &
        (df_lancamentos['data_lancamento'].dt.date <= data_fim)
    ].copy()

    # Calcular saldo anterior (antes do período)
    df_anterior = df_lancamentos[
        df_lancamentos['data_lancamento'].dt.date < data_inicio
    ].copy()

    # Normalizar códigos de conta (remover .0 se for float)
    if 'reduz_deb' in df_periodo.columns:
        df_periodo['reduz_deb'] = df_periodo['reduz_deb'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
        )
    if 'reduz_cred' in df_periodo.columns:
        df_periodo['reduz_cred'] = df_periodo['reduz_cred'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
        )

    if not df_anterior.empty:
        if 'reduz_deb' in df_anterior.columns:
            df_anterior['reduz_deb'] = df_anterior['reduz_deb'].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
            )
        if 'reduz_cred' in df_anterior.columns:
            df_anterior['reduz_cred'] = df_anterior['reduz_cred'].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
            )

    # Calcular totais por conta
    balancete = []

    # Obter todas as contas usadas no período e anteriormente
    contas_periodo_deb = df_periodo['reduz_deb'].dropna().unique() if 'reduz_deb' in df_periodo.columns else []
    contas_periodo_cred = df_periodo['reduz_cred'].dropna().unique() if 'reduz_cred' in df_periodo.columns else []
    contas_anterior_deb = df_anterior['reduz_deb'].dropna().unique() if not df_anterior.empty and 'reduz_deb' in df_anterior.columns else []
    contas_anterior_cred = df_anterior['reduz_cred'].dropna().unique() if not df_anterior.empty and 'reduz_cred' in df_anterior.columns else []

    todas_contas = set(list(contas_periodo_deb) + list(contas_periodo_cred) +
                      list(contas_anterior_deb) + list(contas_anterior_cred))

    for conta in sorted(todas_contas, key=lambda x: str(x)):
        # Converter conta para string (sem decimais se for número)
        conta_str = str(conta)

        # Buscar nome e tipo da conta no plano de contas
        conta_info = df_plano_contas[df_plano_contas['codigo'] == conta_str]
        nome_conta = conta_info.iloc[0]['descricao'] if not conta_info.empty else 'N/A'
        tipo_conta = conta_info.iloc[0]['tipo'] if not conta_info.empty and 'tipo' in conta_info.columns else 'Analitico'

        # Calcular saldo anterior
        if not df_anterior.empty:
            saldo_ant_deb = df_anterior[df_anterior['reduz_deb'] == conta_str]['valor'].sum()
            saldo_ant_cred = df_anterior[df_anterior['reduz_cred'] == conta_str]['valor'].sum()
            saldo_anterior = saldo_ant_deb - saldo_ant_cred
        else:
            saldo_anterior = 0.0

        # Calcular débitos e créditos do período
        debitos = df_periodo[df_periodo['reduz_deb'] == conta_str]['valor'].sum()
        creditos = df_periodo[df_periodo['reduz_cred'] == conta_str]['valor'].sum()
        saldo_periodo = debitos - creditos
        saldo_final = saldo_anterior + saldo_periodo

        balancete.append({
            'Conta': conta_str,
            'Descrição': nome_conta[:40],  # Limitar tamanho
            'Saldo Anterior': saldo_anterior,
            'Débitos': debitos,
            'Créditos': creditos,
            'Saldo Final': saldo_final,
            'Tipo': tipo_conta
        })

    # Criar DataFrame do balancete
    df_balancete = pd.DataFrame(balancete)

    if df_balancete.empty:
        elements.append(Paragraph("Nenhum lançamento encontrado no período.", getSampleStyleSheet()['Normal']))
    else:
        # Criar tabela com design moderno incluindo Saldo Anterior
        data = [['Conta', 'Descrição', 'Saldo Ant. (R$)', 'Débitos (R$)', 'Créditos (R$)', 'Saldo Final (R$)']]

        for _, row in df_balancete.iterrows():
            # Aplicar negrito se conta for sintética
            if row.get('Tipo') == 'Sintetico':
                from reportlab.lib.styles import getSampleStyleSheet
                from reportlab.platypus import Paragraph
                style_bold = getSampleStyleSheet()['BodyText']
                style_bold.fontName = 'Helvetica-Bold'
                style_bold.fontSize = 8

                data.append([
                    Paragraph(f"<b>{row['Conta']}</b>", style_bold),
                    Paragraph(f"<b>{row['Descrição']}</b>", style_bold),
                    Paragraph(f"<b>{row['Saldo Anterior']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') + "</b>", style_bold),
                    Paragraph(f"<b>{row['Débitos']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') + "</b>", style_bold),
                    Paragraph(f"<b>{row['Créditos']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') + "</b>", style_bold),
                    Paragraph(f"<b>{row['Saldo Final']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') + "</b>", style_bold)
                ])
            else:
                data.append([
                    row['Conta'],
                    row['Descrição'],
                    f"{row['Saldo Anterior']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
                    f"{row['Débitos']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
                    f"{row['Créditos']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
                    f"{row['Saldo Final']:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                ])

        # Totais
        total_saldo_ant = df_balancete['Saldo Anterior'].sum()
        total_debitos = df_balancete['Débitos'].sum()
        total_creditos = df_balancete['Créditos'].sum()
        total_saldo_final = df_balancete['Saldo Final'].sum()

        data.append([
            '', 'TOTAL',
            f"{total_saldo_ant:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
            f"{total_debitos:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
            f"{total_creditos:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'),
            f"{total_saldo_final:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        ])

        table = Table(data, colWidths=[15*mm, 60*mm, 24*mm, 24*mm, 24*mm, 24*mm])

        # Estilo moderno com zebra stripes
        table_style = [
            # Cabeçalho
            ('BACKGROUND', (0, 0), (-1, 0), COR_FUNDO_HEADER),
            ('TEXTCOLOR', (0, 0), (-1, 0), COR_TEXTO_HEADER),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('TOPPADDING', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 10),

            # Corpo da tabela
            ('ALIGN', (0, 1), (0, -2), 'CENTER'),  # Coluna Conta
            ('ALIGN', (1, 1), (1, -2), 'LEFT'),    # Coluna Descrição
            ('ALIGN', (2, 1), (-1, -2), 'RIGHT'),  # Colunas de valores
            ('FONTSIZE', (0, 1), (-1, -2), 9),
            ('TOPPADDING', (0, 1), (-1, -2), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -2), 6),

            # Linha de totais
            ('BACKGROUND', (0, -1), (-1, -1), COR_TOTAL),
            ('TEXTCOLOR', (0, -1), (-1, -1), COR_PRINCIPAL),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, -1), (-1, -1), 10),
            ('ALIGN', (1, -1), (1, -1), 'CENTER'),
            ('ALIGN', (2, -1), (-1, -1), 'RIGHT'),
            ('TOPPADDING', (0, -1), (-1, -1), 8),
            ('BOTTOMPADDING', (0, -1), (-1, -1), 8),

            # Bordas
            ('LINEABOVE', (0, 0), (-1, 0), 2, COR_FUNDO_HEADER),
            ('LINEBELOW', (0, 0), (-1, 0), 1, COR_BORDA),
            ('LINEABOVE', (0, -1), (-1, -1), 2, COR_SECUNDARIA),
            ('LINEBELOW', (0, -1), (-1, -1), 2, COR_SECUNDARIA),
            ('GRID', (0, 1), (-1, -2), 0.5, COR_BORDA),
        ]

        # Adicionar zebra stripes (linhas alternadas)
        for i in range(1, len(data) - 1):  # Exclui header e total
            if i % 2 == 0:
                table_style.append(('BACKGROUND', (0, i), (-1, i), COR_FUNDO_ZEBRA))

        table.setStyle(TableStyle(table_style))
        elements.append(table)

    # Usar canvas customizado para rodapé
    doc.build(elements, canvasmaker=NumberedCanvas)
    buffer.seek(0)
    return buffer


def gerar_livro_diario_pdf(df_lancamentos: pd.DataFrame, empresa_info: dict,
                           logo_path: str, data_inicio: date, data_fim: date) -> BytesIO:
    """
    Gera PDF do Livro Diário com design moderno.
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                           topMargin=15*mm, bottomMargin=25*mm,
                           leftMargin=15*mm, rightMargin=15*mm)

    elements = []
    periodo_str = f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}"

    criar_cabecalho_relatorio(elements, empresa_info, logo_path,
                              "LIVRO DIÁRIO", periodo_str)

    # Converter data_lancamento para datetime se necessário
    if not pd.api.types.is_datetime64_any_dtype(df_lancamentos['data_lancamento']):
        df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'])

    # Filtrar e ordenar lançamentos
    df_periodo = df_lancamentos[
        (df_lancamentos['data_lancamento'].dt.date >= data_inicio) &
        (df_lancamentos['data_lancamento'].dt.date <= data_fim)
    ].copy()

    # Tentar ordenar por idlancamento se existir, senão apenas por data
    if 'idlancamento' in df_periodo.columns:
        df_periodo = df_periodo.sort_values(['data_lancamento', 'idlancamento'])
    else:
        df_periodo = df_periodo.sort_values('data_lancamento')

    if df_periodo.empty:
        elements.append(Paragraph("Nenhum lançamento encontrado no período.", getSampleStyleSheet()['Normal']))
    else:
        # Criar tabela com todos os lançamentos
        data = [['Data', 'Conta Débito', 'Conta Crédito', 'Histórico', 'Valor (R$)']]

        # Função auxiliar para verificar se conta é sintética
        def verificar_tipo_conta(codigo):
            if pd.isna(codigo) or codigo == '':
                return 'Analitico'
            conta_info = df_plano_contas[df_plano_contas['codigo'] == str(codigo)]
            if not conta_info.empty and 'tipo' in conta_info.columns:
                return conta_info.iloc[0]['tipo']
            return 'Analitico'

        style_bold = getSampleStyleSheet()['BodyText']
        style_bold.fontName = 'Helvetica-Bold'
        style_bold.fontSize = 8

        for _, row in df_periodo.iterrows():
            data_lanc = row['data_lancamento'].strftime('%d/%m/%Y')
            historico = str(row.get('historico', ''))[:50]
            conta_deb = str(row.get('reduz_deb', ''))[:30] if pd.notna(row.get('reduz_deb')) else ''
            conta_cred = str(row.get('reduz_cred', ''))[:30] if pd.notna(row.get('reduz_cred')) else ''
            valor = row.get('valor', 0.0)

            # Verificar tipo das contas
            tipo_deb = verificar_tipo_conta(conta_deb)
            tipo_cred = verificar_tipo_conta(conta_cred)

            # Aplicar negrito se sintética
            if tipo_deb == 'Sintetico':
                conta_deb_display = Paragraph(f"<b>{conta_deb}</b>", style_bold)
            else:
                conta_deb_display = conta_deb

            if tipo_cred == 'Sintetico':
                conta_cred_display = Paragraph(f"<b>{conta_cred}</b>", style_bold)
            else:
                conta_cred_display = conta_cred

            data.append([
                data_lanc,
                conta_deb_display,
                conta_cred_display,
                historico,
                f"{valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            ])

        table = Table(data, colWidths=[22*mm, 45*mm, 45*mm, 50*mm, 20*mm])

        # Estilo moderno
        table_style = [
            # Cabeçalho
            ('BACKGROUND', (0, 0), (-1, 0), COR_FUNDO_HEADER),
            ('TEXTCOLOR', (0, 0), (-1, 0), COR_TEXTO_HEADER),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('TOPPADDING', (0, 0), (-1, 0), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),

            # Corpo
            ('FONTSIZE', (0, 1), (-1, -1), 7),
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),
            ('ALIGN', (1, 1), (3, -1), 'LEFT'),
            ('ALIGN', (4, 1), (4, -1), 'RIGHT'),
            ('TOPPADDING', (0, 1), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 5),

            # Bordas
            ('LINEBELOW', (0, 0), (-1, 0), 1, COR_BORDA),
            ('GRID', (0, 1), (-1, -1), 0.5, COR_BORDA),
        ]

        # Zebra stripes
        for i in range(1, len(data)):
            if i % 2 == 0:
                table_style.append(('BACKGROUND', (0, i), (-1, i), COR_FUNDO_ZEBRA))

        table.setStyle(TableStyle(table_style))

        elements.append(table)

    doc.build(elements, canvasmaker=NumberedCanvas)
    buffer.seek(0)
    return buffer


def gerar_livro_razao_pdf(df_lancamentos: pd.DataFrame, df_plano_contas: pd.DataFrame,
                          empresa_info: dict, logo_path: str, conta_codigo: str,
                          data_inicio: date, data_fim: date) -> BytesIO:
    """
    Gera PDF do Livro Razão para uma conta específica com design moderno.
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                           topMargin=15*mm, bottomMargin=25*mm,
                           leftMargin=15*mm, rightMargin=15*mm)

    elements = []

    # Buscar nome e tipo da conta
    conta_info = df_plano_contas[df_plano_contas['codigo'] == str(conta_codigo)]
    nome_conta = conta_info.iloc[0]['descricao'] if not conta_info.empty else 'N/A'
    tipo_conta = conta_info.iloc[0]['tipo'] if not conta_info.empty and 'tipo' in conta_info.columns else 'Analitico'

    # Aplicar negrito no nome da conta se for sintética
    if tipo_conta == 'Sintetico':
        periodo_str = f"<b>Conta: {conta_codigo} - {nome_conta}</b><br/>Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}"
    else:
        periodo_str = f"Conta: {conta_codigo} - {nome_conta}<br/>Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}"

    criar_cabecalho_relatorio(elements, empresa_info, logo_path,
                              "LIVRO RAZÃO", periodo_str)

    # Converter data_lancamento para datetime se necessário
    if not pd.api.types.is_datetime64_any_dtype(df_lancamentos['data_lancamento']):
        df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'])

    # Filtrar lançamentos da conta
    df_periodo = df_lancamentos[
        (df_lancamentos['data_lancamento'].dt.date >= data_inicio) &
        (df_lancamentos['data_lancamento'].dt.date <= data_fim)
    ].copy()

    # Normalizar código da conta
    conta_codigo_str = str(int(float(conta_codigo))) if str(conta_codigo).replace('.', '').replace('-', '').isdigit() else str(conta_codigo)

    if 'reduz_deb' in df_periodo.columns:
        df_periodo['reduz_deb'] = df_periodo['reduz_deb'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
        )
    if 'reduz_cred' in df_periodo.columns:
        df_periodo['reduz_cred'] = df_periodo['reduz_cred'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
        )

    # Filtrar pela conta (usar reduz_deb e reduz_cred)
    df_conta = df_periodo[
        (df_periodo['reduz_deb'] == conta_codigo_str) |
        (df_periodo['reduz_cred'] == conta_codigo_str)
    ].sort_values('data_lancamento')

    if df_conta.empty:
        elements.append(Paragraph("Nenhum lançamento encontrado para esta conta no período.", getSampleStyleSheet()['Normal']))
    else:
        # Criar tabela
        data = [['Data', 'Histórico', 'Débito (R$)', 'Crédito (R$)', 'Saldo (R$)']]

        saldo = 0
        for _, row in df_conta.iterrows():
            data_lanc = row['data_lancamento'].strftime('%d/%m/%Y')
            historico = str(row.get('historico', ''))[:50]

            # Usar valor e verificar se é débito ou crédito
            valor = row.get('valor', 0)
            debito = valor if row.get('reduz_deb') == conta_codigo_str else 0
            credito = valor if row.get('reduz_cred') == conta_codigo_str else 0
            saldo += debito - credito

            data.append([
                data_lanc,
                historico,
                f"{debito:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if debito > 0 else '',
                f"{credito:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if credito > 0 else '',
                f"{saldo:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
            ])

        table = Table(data, colWidths=[22*mm, 80*mm, 25*mm, 25*mm, 30*mm])

        # Estilo moderno
        table_style = [
            # Cabeçalho
            ('BACKGROUND', (0, 0), (-1, 0), COR_FUNDO_HEADER),
            ('TEXTCOLOR', (0, 0), (-1, 0), COR_TEXTO_HEADER),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 9),
            ('TOPPADDING', (0, 0), (-1, 0), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),

            # Corpo
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('ALIGN', (0, 1), (0, -1), 'CENTER'),
            ('ALIGN', (1, 1), (1, -1), 'LEFT'),
            ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
            ('TOPPADDING', (0, 1), (-1, -1), 6),
            ('BOTTOMPADDING', (0, 1), (-1, -1), 6),

            # Bordas
            ('LINEBELOW', (0, 0), (-1, 0), 1, COR_BORDA),
            ('GRID', (0, 1), (-1, -1), 0.5, COR_BORDA),
        ]

        # Zebra stripes
        for i in range(1, len(data)):
            if i % 2 == 0:
                table_style.append(('BACKGROUND', (0, i), (-1, i), COR_FUNDO_ZEBRA))

        # Destacar última linha (saldo final)
        if len(data) > 1:
            table_style.append(('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#dbeafe')))
            table_style.append(('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'))

        table.setStyle(TableStyle(table_style))

        elements.append(table)

    doc.build(elements, canvasmaker=NumberedCanvas)
    buffer.seek(0)
    return buffer


def gerar_balanco_patrimonial_pdf(df_lancamentos: pd.DataFrame, df_plano_contas: pd.DataFrame,
                                   empresa_info: dict, logo_path: str, data_referencia: date) -> BytesIO:
    """
    Gera PDF do Balanço Patrimonial com design moderno.
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                           topMargin=15*mm, bottomMargin=25*mm,
                           leftMargin=15*mm, rightMargin=15*mm)

    elements = []
    periodo_str = f"Data de Referência: {data_referencia.strftime('%d/%m/%Y')}"

    criar_cabecalho_relatorio(elements, empresa_info, logo_path,
                              "BALANÇO PATRIMONIAL", periodo_str)

    # Converter data_lancamento para datetime se necessário
    if not pd.api.types.is_datetime64_any_dtype(df_lancamentos['data_lancamento']):
        df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'])

    # Filtrar lançamentos até a data de referência
    df_periodo = df_lancamentos[
        df_lancamentos['data_lancamento'].dt.date <= data_referencia
    ].copy()

    # Normalizar códigos de conta
    if 'reduz_deb' in df_periodo.columns:
        df_periodo['reduz_deb'] = df_periodo['reduz_deb'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
        )
    if 'reduz_cred' in df_periodo.columns:
        df_periodo['reduz_cred'] = df_periodo['reduz_cred'].apply(
            lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
        )

    # Calcular saldos por conta
    saldos = {}
    for conta in df_plano_contas['codigo'].unique():
        # Usar reduz_deb e reduz_cred
        debitos = df_periodo[df_periodo['reduz_deb'] == conta]['valor'].sum()
        creditos = df_periodo[df_periodo['reduz_cred'] == conta]['valor'].sum()
        saldo = debitos - creditos

        if abs(saldo) > 0.01:  # Apenas contas com saldo
            conta_info = df_plano_contas[df_plano_contas['codigo'] == conta].iloc[0]

            # Classificar por código da conta (padrão contábil brasileiro)
            codigo_str = str(conta)
            primeiro_digito = codigo_str[0] if len(codigo_str) > 0 else ''
            tipo_conta = conta_info['tipo'] if 'tipo' in df_plano_contas.columns else 'Analitico'
            classificacao = conta_info['classificacao'] if 'classificacao' in df_plano_contas.columns else ''

            saldos[conta] = {
                'descricao': conta_info['descricao'],
                'primeiro_digito': primeiro_digito,
                'saldo': saldo,
                'tipo': tipo_conta,
                'classificacao': classificacao
            }

    # Separar Ativo, Passivo e Patrimônio Líquido
    ativo_data = [['Conta', 'Classificação', 'Descrição', 'Valor (R$)']]
    passivo_data = [['Conta', 'Classificação', 'Descrição', 'Valor (R$)']]

    total_ativo = 0
    total_passivo_pl = 0

    style_bold = getSampleStyleSheet()['BodyText']
    style_bold.fontName = 'Helvetica-Bold'
    style_bold.fontSize = 7

    for conta, info in sorted(saldos.items()):
        valor_formatado = f"{abs(info['saldo']):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        tipo_conta = info.get('tipo', 'Analitico')
        classificacao_conta = info.get('classificacao', '')

        # Classificar por código da conta
        # 1 = Ativo, 2 = Passivo, 3 = Patrimônio Líquido
        if info['primeiro_digito'] == '1':
            if tipo_conta == 'Sintetico':
                ativo_data.append([
                    Paragraph(f"<b>{str(conta)}</b>", style_bold),
                    Paragraph(f"<b>{classificacao_conta}</b>", style_bold),
                    Paragraph(f"<b>{info['descricao'][:35]}</b>", style_bold),
                    Paragraph(f"<b>{valor_formatado}</b>", style_bold)
                ])
            else:
                ativo_data.append([str(conta), classificacao_conta, info['descricao'][:35], valor_formatado])
            total_ativo += abs(info['saldo'])
        elif info['primeiro_digito'] == '2':
            if tipo_conta == 'Sintetico':
                passivo_data.append([
                    Paragraph(f"<b>{str(conta)}</b>", style_bold),
                    Paragraph(f"<b>{classificacao_conta}</b>", style_bold),
                    Paragraph(f"<b>{info['descricao'][:35]}</b>", style_bold),
                    Paragraph(f"<b>{valor_formatado}</b>", style_bold)
                ])
            else:
                passivo_data.append([str(conta), classificacao_conta, info['descricao'][:35], valor_formatado])
            total_passivo_pl += abs(info['saldo'])
        elif info['primeiro_digito'] == '3':
            if tipo_conta == 'Sintetico':
                passivo_data.append([
                    Paragraph(f"<b>{str(conta)}</b>", style_bold),
                    Paragraph(f"<b>{classificacao_conta}</b>", style_bold),
                    Paragraph(f"<b>{info['descricao'][:35]}</b>", style_bold),
                    Paragraph(f"<b>{valor_formatado}</b>", style_bold)
                ])
            else:
                passivo_data.append([str(conta), classificacao_conta, info['descricao'][:35], valor_formatado])
            total_passivo_pl += abs(info['saldo'])
        # Contas 4, 5, 6, 7 (Receitas/Despesas) não entram no Balanço Patrimonial

    total_passivo = total_passivo_pl  # Para compatibilidade

    # Adicionar totais
    ativo_data.append(['', '', 'TOTAL ATIVO', f"{total_ativo:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')])
    passivo_data.append(['', '', 'TOTAL PASSIVO + PL', f"{total_passivo:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')])

    # Criar tabelas com design moderno (uma abaixo da outra para permitir quebra de página)
    # Larguras: Conta (20mm), Classificação (50mm), Descrição (70mm), Valor (40mm) = 180mm
    ativo_table = Table(ativo_data, colWidths=[20*mm, 50*mm, 70*mm, 40*mm], repeatRows=1)

    # Estilo moderno para Ativo
    ativo_style = [
        # Cabeçalho
        ('BACKGROUND', (0, 0), (-1, 0), COR_FUNDO_HEADER),
        ('TEXTCOLOR', (0, 0), (-1, 0), COR_TEXTO_HEADER),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),

        # Corpo
        ('FONTSIZE', (0, 1), (-1, -2), 7),
        ('ALIGN', (0, 1), (0, -1), 'LEFT'),      # Conta
        ('ALIGN', (1, 1), (1, -1), 'CENTER'),    # Classificação
        ('ALIGN', (2, 1), (2, -1), 'LEFT'),      # Descrição
        ('ALIGN', (3, 1), (3, -1), 'RIGHT'),     # Valor
        ('TOPPADDING', (0, 1), (-1, -2), 5),
        ('BOTTOMPADDING', (0, 1), (-1, -2), 5),

        # Total
        ('BACKGROUND', (0, -1), (-1, -1), COR_TOTAL),
        ('TEXTCOLOR', (0, -1), (-1, -1), COR_PRINCIPAL),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, -1), (-1, -1), 9),
        ('TOPPADDING', (0, -1), (-1, -1), 7),
        ('BOTTOMPADDING', (0, -1), (-1, -1), 7),
        ('ALIGN', (2, -1), (2, -1), 'LEFT'),     # Total alinhado à esquerda na coluna descrição
        ('ALIGN', (3, -1), (3, -1), 'RIGHT'),    # Valor do total à direita

        # Bordas
        ('LINEBELOW', (0, 0), (-1, 0), 1, COR_BORDA),
        ('LINEABOVE', (0, -1), (-1, -1), 2, COR_SECUNDARIA),
        ('LINEBELOW', (0, -1), (-1, -1), 2, COR_SECUNDARIA),
        ('GRID', (0, 1), (-1, -2), 0.5, COR_BORDA),
    ]

    # Zebra stripes para Ativo
    for i in range(1, len(ativo_data) - 1):
        if i % 2 == 0:
            ativo_style.append(('BACKGROUND', (0, i), (-1, i), COR_FUNDO_ZEBRA))

    ativo_table.setStyle(TableStyle(ativo_style))

    passivo_table = Table(passivo_data, colWidths=[20*mm, 50*mm, 70*mm, 40*mm], repeatRows=1)

    # Estilo moderno para Passivo
    passivo_style = [
        # Cabeçalho
        ('BACKGROUND', (0, 0), (-1, 0), COR_FUNDO_HEADER),
        ('TEXTCOLOR', (0, 0), (-1, 0), COR_TEXTO_HEADER),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('TOPPADDING', (0, 0), (-1, 0), 8),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),

        # Corpo
        ('FONTSIZE', (0, 1), (-1, -2), 7),
        ('ALIGN', (0, 1), (0, -1), 'LEFT'),      # Conta
        ('ALIGN', (1, 1), (1, -1), 'CENTER'),    # Classificação
        ('ALIGN', (2, 1), (2, -1), 'LEFT'),      # Descrição
        ('ALIGN', (3, 1), (3, -1), 'RIGHT'),     # Valor
        ('TOPPADDING', (0, 1), (-1, -2), 5),
        ('BOTTOMPADDING', (0, 1), (-1, -2), 5),

        # Total
        ('BACKGROUND', (0, -1), (-1, -1), COR_TOTAL),
        ('TEXTCOLOR', (0, -1), (-1, -1), COR_PRINCIPAL),
        ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, -1), (-1, -1), 9),
        ('TOPPADDING', (0, -1), (-1, -1), 7),
        ('BOTTOMPADDING', (0, -1), (-1, -1), 7),
        ('ALIGN', (2, -1), (2, -1), 'LEFT'),     # Total alinhado à esquerda na coluna descrição
        ('ALIGN', (3, -1), (3, -1), 'RIGHT'),    # Valor do total à direita

        # Bordas
        ('LINEBELOW', (0, 0), (-1, 0), 1, COR_BORDA),
        ('LINEABOVE', (0, -1), (-1, -1), 2, COR_SECUNDARIA),
        ('LINEBELOW', (0, -1), (-1, -1), 2, COR_SECUNDARIA),
        ('GRID', (0, 1), (-1, -2), 0.5, COR_BORDA),
    ]

    # Zebra stripes para Passivo
    for i in range(1, len(passivo_data) - 1):
        if i % 2 == 0:
            passivo_style.append(('BACKGROUND', (0, i), (-1, i), COR_FUNDO_ZEBRA))

    passivo_table.setStyle(TableStyle(passivo_style))

    # Adicionar tabelas separadamente para permitir quebra de página
    elements.append(Paragraph("<b>ATIVO</b>", getSampleStyleSheet()['Heading2']))
    elements.append(Spacer(1, 3*mm))
    elements.append(ativo_table)
    elements.append(Spacer(1, 10*mm))

    elements.append(Paragraph("<b>PASSIVO E PATRIMÔNIO LÍQUIDO</b>", getSampleStyleSheet()['Heading2']))
    elements.append(Spacer(1, 3*mm))
    elements.append(passivo_table)

    doc.build(elements, canvasmaker=NumberedCanvas)
    buffer.seek(0)
    return buffer
