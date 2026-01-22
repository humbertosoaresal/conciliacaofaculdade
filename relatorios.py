# relatorios.py
import pandas as pd
import streamlit as st
from datetime import date
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib import colors
from reportlab.lib.units import mm
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from io import BytesIO
import os

# CORREÇÃO: Importação Absoluta
from utils import to_excel


def gerar_dados_relatorio(df_ofx_conc: pd.DataFrame, df_contabil_conc: pd.DataFrame,
                          tipo_relatorio: str) -> pd.DataFrame:
    """Gera e retorna o DataFrame para o relatório solicitado com base no status da conciliação."""

    if tipo_relatorio == "Conciliados":

        df_export = df_ofx_conc[df_ofx_conc['Conciliado_Contábil'] == 'Sim'].copy()

        df_contabil_match = df_contabil_conc[['ID Contabil', 'Data', 'Historico', 'Conta Contábil', 'Valor']].copy()
        df_contabil_match.rename(columns={'Data': 'Data Contábil',
                                          'Historico': 'Historico Contábil',
                                          'Conta Contábil': 'Conta Contábil Conciliada',
                                          'Valor': 'Valor Contábil'}, inplace=True)

        df_export = df_export.merge(
            df_contabil_match,
            left_on='ID_Contabil_Conciliado',
            right_on='ID Contabil',
            how='left'
        )

        df_export = df_export[[
            'Data Lançamento', 'Data Contábil', 'Valor', 'Valor Contábil',
            'Descrição', 'Historico Contábil', 'Conta_Contábil_Vinculada',
            'Conta Contábil Conciliada', 'Banco_OFX', 'Passagem_Conciliacao'
        ]].rename(columns={
            'Data Lançamento': 'Data OFX',
            'Valor': 'Valor OFX',
            'Descrição': 'Detalhe OFX',
            'Conta_Contábil_Vinculada': 'Conta Contábil Prevista',
        })

    elif tipo_relatorio == "Sobrantes":

        # 1. Sobrantes no OFX
        df_ofx_sob = df_ofx_conc[df_ofx_conc['Conciliado_Contábil'] == 'Não'][[
            'Data Lançamento', 'Valor', 'Descrição', 'Banco_OFX', 'Conta_Contábil_Vinculada']].rename(columns={
            'Descrição': 'Detalhe', 'Data Lançamento': 'Data', 'Conta_Contábil_Vinculada': 'Conta Contábil'}).assign(
            Origem='Extrato Bancário (OFX)')

        # 2. Sobrantes no Contábil
        df_contabil_sob = df_contabil_conc[df_contabil_conc['Conciliado_OFX'] == 'Não'][[
            'Data', 'Valor', 'Historico', 'Conta Contábil']].rename(columns={
            'Historico': 'Detalhe'}).assign(Origem='Extrato Contábil', Banco_OFX='N/A')

        # 3. Combina e ordena
        df_export = pd.concat([df_ofx_sob, df_contabil_sob]).sort_values(by=['Data', 'Origem'])

    elif tipo_relatorio == "Analítico":
        # Relatório Analítico: Resumo por conta e status

        conciliados = df_ofx_conc[df_ofx_conc['Conciliado_Contábil'] == 'Sim'].shape[0]
        sobrantes_ofx = df_ofx_conc[df_ofx_conc['Conciliado_Contábil'] == 'Não'].shape[0]
        sobrantes_contabil = df_contabil_conc[df_contabil_conc['Conciliado_OFX'] == 'Não'].shape[0]

        df_export = pd.DataFrame({
            'Item': ['Conciliações Realizadas', 'Sobrantes (Extrato Bancário)', 'Sobrantes (Extrato Contábil)',
                     'Total de Transações OFX', 'Total de Transações Contábil'],
            'Quantidade': [conciliados, sobrantes_ofx, sobrantes_contabil, len(df_ofx_conc), len(df_contabil_conc)],
            'Valor Total Absoluto': [
                df_ofx_conc[df_ofx_conc['Conciliado_Contábil'] == 'Sim']['Valor'].abs().sum(),
                df_ofx_conc[df_ofx_conc['Conciliado_Contábil'] == 'Não']['Valor'].abs().sum(),
                df_contabil_conc[df_contabil_conc['Conciliado_OFX'] == 'Não']['Valor'].abs().sum(),
                df_ofx_conc['Valor'].abs().sum(),
                df_contabil_conc['Valor'].abs().sum(),
            ]
        })

    else:
        return pd.DataFrame()

    return df_export


def gerar_extrato_bancario_pdf(df_extrato: pd.DataFrame, info_conta: dict, data_inicio: date, data_fim: date) -> BytesIO:
    """
    Gera um PDF de extrato bancário no formato Sicredi.

    Args:
        df_extrato: DataFrame com as transações do extrato
        info_conta: Dicionário com informações da conta (Associado, Cooperativa, Conta, Codigo_Banco, Path_Logo)
        data_inicio: Data inicial do período
        data_fim: Data final do período

    Returns:
        BytesIO com o PDF gerado
    """
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=A4,
                           topMargin=15*mm, bottomMargin=15*mm,
                           leftMargin=15*mm, rightMargin=15*mm)

    elements = []
    styles = getSampleStyleSheet()

    # Estilo customizado para o cabeçalho
    style_header = ParagraphStyle(
        'CustomHeader',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.black,
        spaceAfter=1,
        leftIndent=0
    )

    style_title = ParagraphStyle(
        'CustomTitle',
        parent=styles['Normal'],
        fontSize=11,
        textColor=colors.HexColor('#66B32E'),
        fontName='Helvetica-Bold',
        spaceAfter=8,
        leftIndent=0
    )

    # Logo do banco (à esquerda, como no original)
    logo_path = info_conta.get('Path_Logo', 'logos/default.png')

    # Se for Sicredi (748), garantir que usa o logo correto
    codigo_banco = info_conta.get('Codigo_Banco', '')
    if codigo_banco == '748':
        logo_path = 'logos/sicredi.png'

    if os.path.exists(logo_path):
        # Tamanho do logo GRANDE como no original do Sicredi
        # Aumentado em 20%: 95mm * 1.2 = 114mm
        logo = Image(logo_path, width=114*mm, height=40*mm, kind='proportional')

        # Criar tabela com logo à esquerda (sem centralizar)
        logo_table = Table([[logo]], colWidths=[180*mm])
        logo_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),  # Logo à esquerda
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
            ('TOPPADDING', (0, 0), (-1, -1), 0),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
        ]))
        elements.append(logo_table)
        elements.append(Spacer(1, 4*mm))

    # Informações da conta com moldura (como no original)
    info_box_style = ParagraphStyle(
        'InfoBox',
        parent=styles['Normal'],
        fontSize=9,
        textColor=colors.black,
        leftIndent=0,
        spaceAfter=1,
        alignment=TA_LEFT
    )

    # Criar tabela para a caixa de informações (sem indentação/padding extra)
    info_data = [
        [Paragraph(f"<b>Associado:</b> {info_conta.get('Associado', 'N/A')}", info_box_style)],
        [Paragraph(f"<b>Cooperativa:</b> {info_conta.get('Cooperativa', info_conta.get('Agencia', 'N/A'))}", info_box_style)],
        [Paragraph(f"<b>Conta:</b> {info_conta.get('Conta', 'N/A')}", info_box_style)]
    ]

    info_table = Table(info_data, colWidths=[180*mm])
    info_table.setStyle(TableStyle([
        ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#CCCCCC')),  # Borda mais suave
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
    ]))

    elements.append(info_table)
    elements.append(Spacer(1, 5*mm))

    # Título do extrato com período (período em cinza)
    periodo_str = f"(Período de {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')})"
    elements.append(Paragraph(f"<font color='#66B32E'><b>Extrato</b></font>  <font color='#999999'>{periodo_str}</font>", style_title))
    elements.append(Spacer(1, 4*mm))

    # Preparar dados da tabela
    table_data = [['Data', 'Descrição', 'Documento', 'Valor (R$)', 'Saldo (R$)']]

    # Saldo inicial
    saldo_inicial = info_conta.get('Saldo Inicial', 0.0)
    saldo_acumulado = saldo_inicial

    # Linha de saldo anterior
    table_data.append(['', 'SALDO ANTERIOR', '', '', f'{saldo_inicial:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')])

    # Adicionar transações
    if not df_extrato.empty:
        for _, row in df_extrato.iterrows():
            data_transacao = row.get('Data Lançamento', '')
            if isinstance(data_transacao, pd.Timestamp):
                data_str = data_transacao.strftime('%d/%m/%Y')
            elif isinstance(data_transacao, date):
                data_str = data_transacao.strftime('%d/%m/%Y')
            else:
                data_str = str(data_transacao)

            descricao = str(row.get('Descrição', ''))[:60]  # Limita descrição
            documento = str(row.get('ID Transacao', ''))[-6:] if row.get('ID Transacao') else ''
            valor = float(row.get('Valor', 0))
            saldo_acumulado += valor

            # Formatação de valores com sinal negativo visível
            if valor < 0:
                valor_str = f'-{abs(valor):,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')
            else:
                valor_str = f'{valor:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

            saldo_str = f'{saldo_acumulado:,.2f}'.replace(',', 'X').replace('.', ',').replace('X', '.')

            table_data.append([data_str, descricao, documento, valor_str, saldo_str])

    # Criar tabela com larguras ajustadas (igual ao original)
    col_widths = [20*mm, 90*mm, 25*mm, 22*mm, 22*mm]
    table = Table(table_data, colWidths=col_widths, repeatRows=1)

    # Estilo da tabela (replicando exatamente o original Sicredi)
    table_style = TableStyle([
        # ===== CABEÇALHO =====
        # Verde mais suave/claro como no original
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#7CB342')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('ALIGN', (0, 0), (0, 0), 'LEFT'),
        ('ALIGN', (1, 0), (1, 0), 'LEFT'),
        ('ALIGN', (2, 0), (2, 0), 'CENTER'),
        ('ALIGN', (3, 0), (-1, 0), 'RIGHT'),
        ('TOPPADDING', (0, 0), (-1, 0), 7),
        ('BOTTOMPADDING', (0, 0), (-1, 0), 7),
        ('LEFTPADDING', (0, 0), (-1, 0), 8),
        ('RIGHTPADDING', (0, 0), (-1, 0), 8),

        # Linhas verticais brancas no cabeçalho (separadores de coluna)
        ('LINEAFTER', (0, 0), (0, 0), 2, colors.white),
        ('LINEAFTER', (1, 0), (1, 0), 2, colors.white),
        ('LINEAFTER', (2, 0), (2, 0), 2, colors.white),
        ('LINEAFTER', (3, 0), (3, 0), 2, colors.white),

        # Linha horizontal inferior do cabeçalho (verde mais escuro)
        ('LINEBELOW', (0, 0), (-1, 0), 3, colors.HexColor('#689F38')),

        # ===== LINHA SALDO ANTERIOR =====
        # Fundo cinza MUITO claro (como no original)
        ('BACKGROUND', (0, 1), (-1, 1), colors.HexColor('#F5F5F5')),
        ('FONTNAME', (0, 1), (-1, 1), 'Helvetica'),
        ('FONTSIZE', (0, 1), (-1, 1), 8),
        ('TEXTCOLOR', (0, 1), (-1, 1), colors.HexColor('#BBBBBB')),
        ('ALIGN', (1, 1), (1, 1), 'LEFT'),
        ('ALIGN', (4, 1), (4, 1), 'RIGHT'),
        ('TOPPADDING', (0, 1), (-1, 1), 6),
        ('BOTTOMPADDING', (0, 1), (-1, 1), 6),
        ('LEFTPADDING', (0, 1), (-1, 1), 8),
        ('RIGHTPADDING', (0, 1), (-1, 1), 8),

        # ===== DADOS DAS TRANSAÇÕES =====
        ('FONTNAME', (0, 2), (-1, -1), 'Helvetica'),
        ('FONTSIZE', (0, 2), (-1, -1), 8),
        ('TEXTCOLOR', (0, 2), (-1, -1), colors.black),
        ('ALIGN', (0, 2), (0, -1), 'CENTER'),
        ('ALIGN', (1, 2), (1, -1), 'LEFT'),
        ('ALIGN', (2, 2), (2, -1), 'CENTER'),
        ('ALIGN', (3, 2), (3, -1), 'RIGHT'),
        ('ALIGN', (4, 2), (4, -1), 'RIGHT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),

        # Padding nas linhas de dados
        ('TOPPADDING', (0, 2), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 2), (-1, -1), 6),
        ('LEFTPADDING', (0, 2), (-1, -1), 8),
        ('RIGHTPADDING', (0, 2), (-1, -1), 8),

        # ===== BORDAS =====
        # SEM bordas laterais (verticais)
        # Linhas horizontais sutis entre as linhas de dados
        ('LINEBELOW', (0, 1), (-1, -1), 0.5, colors.HexColor('#E0E0E0')),
    ])

    # Aplicar zebrado (linhas alternadas em cinza claro)
    for i in range(2, len(table_data)):
        if i % 2 == 0:  # Linhas pares (exceto cabeçalho e saldo anterior)
            table_style.add('BACKGROUND', (0, i), (-1, i), colors.HexColor('#FAFAFA'))
        else:  # Linhas ímpares
            table_style.add('BACKGROUND', (0, i), (-1, i), colors.white)

    table.setStyle(table_style)
    elements.append(table)

    # Rodapé com informações de contato
    elements.append(Spacer(1, 10*mm))
    style_footer = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#666666'),
        alignment=TA_CENTER
    )

    codigo_banco = info_conta.get('Codigo_Banco', '')
    if codigo_banco == '748':  # Sicredi
        elements.append(Paragraph("Sicredi Fone 0800 724 4770", style_footer))
        elements.append(Paragraph("SAC 0800 724 7220", style_footer))
        elements.append(Paragraph("Ouvidoria 0800 646 2519", style_footer))

    # Gerar PDF
    doc.build(elements)
    buffer.seek(0)
    return buffer