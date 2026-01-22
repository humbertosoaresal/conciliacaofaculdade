# app.py (Versão Final e Corrigida)
import streamlit as st
import pandas as pd
import datetime
from datetime import date
import os
from io import BytesIO
import numpy as np
import uuid
import re

# ==============================================================================
# IMPORTAÇÕES ABSOLUTAS
# ==============================================================================
from config import COL_CONFIG
from utils import safe_parse_date, to_excel, formatar_dataframe_para_exibicao, convert_df_to_csv, create_word_report
from data_loader import ler_cadastro_contas, importar_multiplos_extratos, ler_extrato_contabil, ler_bancos_associados, ler_plano_contas_csv
from conciliacao import vincular_contas_ao_extrato, conciliar_extratos, gerar_lancamentos_saldo_negativo, gerar_lancamentos_saldo_negativo_contabil_cadastro
from relatorios import gerar_extrato_bancario_pdf
from relatorios_contabeis import (
    gerar_balancete_pdf,
    gerar_livro_diario_pdf,
    gerar_livro_razao_pdf,
    gerar_balanco_patrimonial_pdf
)
from db_manager import (
    carregar_cadastro_contas,
    salvar_cadastro_contas,
    salvar_contas_ofx_faltantes,
    init_db,
    salvar_extrato_bancario_historico,
    carregar_extrato_bancario_historico,
    limpar_extrato_bancario_historico,
    excluir_conta_cadastro,
    carregar_plano_contas,
    salvar_plano_contas,
    excluir_conta_plano,
    salvar_lancamentos_contabeis,
    carregar_lancamentos_contabeis,
    limpar_lancamentos_contabeis,
    salvar_lancamentos_editados,
    excluir_lancamentos_por_ids,
    salvar_partidas_lancamento,
    excluir_lancamentos_por_idlancamentos,
    carregar_empresa,
    salvar_empresa,
    carregar_socios,
    salvar_socio,
    atualizar_socio,
    excluir_socio,
    carregar_logotipos,
    salvar_logotipo,
    definir_logo_principal,
    excluir_logotipo,
    obter_logo_principal,
    # Parcelamentos
    carregar_parcelamentos,
    salvar_parcelamento,
    atualizar_parcelamento,
    excluir_parcelamento,
    carregar_parcelamento_por_id,
    carregar_debitos_parcelamento,
    salvar_debitos_parcelamento,
    carregar_parcelas_parcelamento,
    salvar_parcelas_parcelamento,
    atualizar_parcela,
    carregar_pagamentos_parcelamento,
    salvar_pagamento_parcelamento,
    atualizar_saldo_parcelamento
)
from cnpj_api import buscar_cnpj_api, formatar_cnpj, limpar_cnpj
from parcelamentos import (
    parse_extrato_parcelamento_ecac,
    parse_arquivo_parcelamento,
    gerar_lancamentos_parcelamento,
    conciliar_parcela_extrato
)

# ==============================================================================
# FUNÇÕES DE UTILIDADE E SUBMENUS
# ==============================================================================
def formatar_moeda(valor):
    if pd.isna(valor) or valor is None:
        return "R$ 0,00"
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def tela_cadastro_empresa():
    """Tela dedicada para cadastro da empresa."""
    st.title("🏢 Cadastro da Empresa")

    # Carrega dados existentes
    empresa_atual = carregar_empresa()

    # Inicializa session_state para armazenar dados temporários
    if 'dados_empresa_temp' not in st.session_state:
        st.session_state.dados_empresa_temp = {}

    # Container principal com borda
    with st.container():
        st.markdown("### 🔍 Buscar Dados por CNPJ")
        st.info("Digite o CNPJ da empresa e clique em 'Buscar' para preencher os dados automaticamente via Receita Federal.")

        col1, col2, col3 = st.columns([3, 1, 1])
        with col1:
            cnpj_input = st.text_input(
                "CNPJ",
                value=formatar_cnpj(empresa_atual.get('cnpj', '')) if empresa_atual.get('cnpj') else '',
                placeholder="00.000.000/0000-00",
                key="cnpj_input",
                help="Digite apenas os números ou no formato 00.000.000/0000-00"
            )
        with col2:
            buscar_clicked = st.button("🔍 Buscar", key="buscar_cnpj", use_container_width=True, type="primary")
        with col3:
            limpar_clicked = st.button("🗑️ Limpar", key="limpar_form", use_container_width=True)

        # Limpar formulário
        if limpar_clicked:
            st.session_state.dados_empresa_temp = {}
            # Limpar todos os campos do formulário
            for campo in ['razao_social', 'nome_fantasia', 'inscricao_estadual', 'inscricao_municipal',
                         'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'municipio', 'uf',
                         'telefone', 'email', 'data_abertura', 'situacao', 'atividade_principal',
                         'atividades_secundarias']:
                if campo in st.session_state:
                    del st.session_state[campo]
            st.rerun()

        # Buscar dados na API
        if buscar_clicked:
            if not cnpj_input:
                st.error("❌ Por favor, digite um CNPJ antes de buscar.")
            else:
                with st.spinner(f"🔍 Buscando CNPJ: {cnpj_input}..."):
                    dados_api = buscar_cnpj_api(cnpj_input)

                if dados_api:
                    st.session_state.dados_empresa_temp = dados_api
                    # Copiar dados para os campos individuais do formulário
                    for campo, valor in dados_api.items():
                        if campo in ['razao_social', 'nome_fantasia', 'inscricao_estadual', 'inscricao_municipal',
                                     'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'municipio', 'uf',
                                     'telefone', 'email', 'data_abertura', 'situacao', 'atividade_principal',
                                     'atividades_secundarias']:
                            st.session_state[campo] = valor
                    st.success("✅ Dados carregados com sucesso!")
                    st.rerun()
                else:
                    st.warning("⚠️ Não foi possível buscar os dados do CNPJ.")

    st.markdown("---")

    # Usar dados temporários se existirem, senão usar dados atuais do BD
    dados_exibir = st.session_state.dados_empresa_temp if st.session_state.dados_empresa_temp else empresa_atual

    # Formulário de cadastro
    if dados_exibir or cnpj_input:
        with st.container():
            st.markdown("### 📋 Dados da Empresa")

            col1, col2 = st.columns(2)
            with col1:
                razao_social = st.text_input(
                    "Razão Social *",
                    value=dados_exibir.get('razao_social', ''),
                    key="razao_social"
                )
            with col2:
                nome_fantasia = st.text_input(
                    "Nome Fantasia",
                    value=dados_exibir.get('nome_fantasia', ''),
                    key="nome_fantasia"
                )

            col1, col2 = st.columns(2)
            with col1:
                inscricao_estadual = st.text_input(
                    "Inscrição Estadual",
                    value=dados_exibir.get('inscricao_estadual', ''),
                    key="inscricao_estadual"
                )
            with col2:
                inscricao_municipal = st.text_input(
                    "Inscrição Municipal",
                    value=dados_exibir.get('inscricao_municipal', ''),
                    key="inscricao_municipal"
                )

            st.markdown("### 📍 Endereço")

            col1, col2, col3 = st.columns([3, 1, 2])
            with col1:
                logradouro = st.text_input(
                    "Logradouro",
                    value=dados_exibir.get('logradouro', ''),
                    key="logradouro"
                )
            with col2:
                numero = st.text_input(
                    "Número",
                    value=dados_exibir.get('numero', ''),
                    key="numero"
                )
            with col3:
                complemento = st.text_input(
                    "Complemento",
                    value=dados_exibir.get('complemento', ''),
                    key="complemento"
                )

            col1, col2 = st.columns(2)
            with col1:
                bairro = st.text_input(
                    "Bairro",
                    value=dados_exibir.get('bairro', ''),
                    key="bairro"
                )
            with col2:
                cep = st.text_input(
                    "CEP",
                    value=dados_exibir.get('cep', ''),
                    key="cep",
                    placeholder="00000-000"
                )

            col1, col2 = st.columns([3, 1])
            with col1:
                municipio = st.text_input(
                    "Município",
                    value=dados_exibir.get('municipio', ''),
                    key="municipio"
                )
            with col2:
                uf = st.text_input(
                    "UF",
                    value=dados_exibir.get('uf', ''),
                    key="uf",
                    max_chars=2,
                    placeholder="SP"
                )

            st.markdown("### 📞 Contato")

            col1, col2 = st.columns(2)
            with col1:
                telefone = st.text_input(
                    "Telefone",
                    value=dados_exibir.get('telefone', ''),
                    key="telefone",
                    placeholder="(00) 0000-0000"
                )
            with col2:
                email = st.text_input(
                    "E-mail",
                    value=dados_exibir.get('email', ''),
                    key="email",
                    placeholder="empresa@exemplo.com.br"
                )

            st.markdown("### ℹ️ Informações Adicionais")

            col1, col2 = st.columns(2)
            with col1:
                data_abertura = st.text_input(
                    "Data de Abertura",
                    value=dados_exibir.get('data_abertura', ''),
                    key="data_abertura",
                    disabled=True
                )
            with col2:
                situacao = st.text_input(
                    "Situação",
                    value=dados_exibir.get('situacao', ''),
                    key="situacao",
                    disabled=True
                )

            st.markdown("### 💼 Atividades Econômicas")

            atividade_principal = st.text_area(
                "Atividade Principal (CNAE)",
                value=dados_exibir.get('atividade_principal', ''),
                key="atividade_principal",
                height=80,
                disabled=True,
                help="Preenchido automaticamente pela busca do CNPJ"
            )

            atividades_secundarias = st.text_area(
                "Atividades Secundárias (CNAE)",
                value=dados_exibir.get('atividades_secundarias', ''),
                key="atividades_secundarias",
                height=120,
                disabled=True,
                help="Preenchido automaticamente pela busca do CNPJ"
            )

            st.markdown("---")

            # Botões de ação
            col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
            with col1:
                if st.button("👥 Sócios", use_container_width=True, type="secondary"):
                    st.session_state.tela_atual = "gerenciar_socios"
                    st.rerun()
            with col2:
                if st.button("🖼️ Logos", use_container_width=True, type="secondary"):
                    st.session_state.tela_atual = "gerenciar_logotipos"
                    st.rerun()
            with col3:
                if st.button("❌ Cancelar", use_container_width=True):
                    st.session_state.dados_empresa_temp = {}
                    st.session_state.tela_atual = None
                    # Limpar todos os campos do formulário
                    for campo in ['razao_social', 'nome_fantasia', 'inscricao_estadual', 'inscricao_municipal',
                                 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'municipio', 'uf',
                                 'telefone', 'email', 'data_abertura', 'situacao', 'atividade_principal',
                                 'atividades_secundarias']:
                        if campo in st.session_state:
                            del st.session_state[campo]
                    st.rerun()
            with col5:
                if st.button("💾 Salvar", use_container_width=True, type="primary"):
                    # Pegar valores do session_state (onde os campos text_input armazenam seus valores)
                    cnpj_salvar = cnpj_input
                    razao_social_salvar = st.session_state.get('razao_social', razao_social)

                    if not cnpj_salvar or not razao_social_salvar:
                        st.error("CNPJ e Razão Social são obrigatórios!")
                    else:
                        # Pegar todos os valores do session_state
                        dados_salvar = {
                            'cnpj': limpar_cnpj(cnpj_salvar),
                            'razao_social': razao_social_salvar,
                            'nome_fantasia': st.session_state.get('nome_fantasia', nome_fantasia),
                            'inscricao_estadual': st.session_state.get('inscricao_estadual', inscricao_estadual),
                            'inscricao_municipal': st.session_state.get('inscricao_municipal', inscricao_municipal),
                            'logradouro': st.session_state.get('logradouro', logradouro),
                            'numero': st.session_state.get('numero', numero),
                            'complemento': st.session_state.get('complemento', complemento),
                            'bairro': st.session_state.get('bairro', bairro),
                            'municipio': st.session_state.get('municipio', municipio),
                            'uf': st.session_state.get('uf', uf),
                            'cep': st.session_state.get('cep', cep),
                            'telefone': st.session_state.get('telefone', telefone),
                            'email': st.session_state.get('email', email),
                            'data_abertura': st.session_state.get('data_abertura', data_abertura),
                            'situacao': st.session_state.get('situacao', situacao),
                            'atividade_principal': st.session_state.get('atividade_principal', atividade_principal),
                            'atividades_secundarias': st.session_state.get('atividades_secundarias', atividades_secundarias)
                        }

                        if salvar_empresa(dados_salvar):
                            st.success("✅ Dados da empresa salvos com sucesso!")
                            st.session_state.dados_empresa_temp = {}
                            st.session_state.tela_atual = None
                            st.balloons()
                            st.rerun()
    else:
        st.info("Digite um CNPJ e clique em 'Buscar' para começar ou preencha os dados manualmente.")

def tela_gerenciar_socios():
    """Tela para gerenciar os sócios da empresa."""
    st.title("👥 Gerenciamento de Sócios")

    # Botão voltar
    if st.button("⬅️ Voltar para Cadastro da Empresa"):
        st.session_state.tela_atual = "cadastro_empresa"
        st.rerun()

    st.markdown("---")

    # Tabs para Adicionar e Listar
    tab1, tab2 = st.tabs(["➕ Adicionar Sócio", "📋 Lista de Sócios"])

    with tab1:
        st.markdown("### Adicionar Novo Sócio")

        with st.form("form_adicionar_socio", clear_on_submit=True):
            st.markdown("##### Dados Pessoais")
            col1, col2 = st.columns(2)
            with col1:
                cpf_socio = st.text_input(
                    "CPF *",
                    placeholder="000.000.000-00",
                    help="Digite apenas os números"
                )
            with col2:
                nome_completo = st.text_input(
                    "Nome Completo *",
                    placeholder="Nome completo do sócio"
                )

            col1, col2 = st.columns(2)
            with col1:
                data_nascimento = st.text_input(
                    "Data de Nascimento",
                    placeholder="DD/MM/AAAA"
                )
            with col2:
                socio_administrador = st.checkbox("Sócio Administrador")

            st.markdown("##### Endereço")
            col1, col2, col3 = st.columns([3, 1, 2])
            with col1:
                logradouro_socio = st.text_input("Logradouro")
            with col2:
                numero_socio = st.text_input("Número")
            with col3:
                complemento_socio = st.text_input("Complemento")

            col1, col2 = st.columns(2)
            with col1:
                bairro_socio = st.text_input("Bairro")
            with col2:
                cep_socio = st.text_input("CEP", placeholder="00000-000")

            col1, col2 = st.columns([3, 1])
            with col1:
                municipio_socio = st.text_input("Município")
            with col2:
                uf_socio = st.text_input("UF", max_chars=2, placeholder="SP")

            st.markdown("##### Contato")
            col1, col2 = st.columns(2)
            with col1:
                telefone_socio = st.text_input("Telefone", placeholder="(00) 00000-0000")
            with col2:
                email_socio = st.text_input("E-mail", placeholder="socio@exemplo.com")

            submitted = st.form_submit_button("💾 Salvar Sócio", use_container_width=True, type="primary")

            if submitted:
                if not cpf_socio or not nome_completo:
                    st.error("CPF e Nome Completo são obrigatórios!")
                else:
                    # Limpar CPF
                    cpf_limpo = re.sub(r'\D', '', cpf_socio)

                    if len(cpf_limpo) != 11:
                        st.error("CPF inválido! Deve ter 11 dígitos.")
                    else:
                        dados_socio = {
                            'cpf': cpf_limpo,
                            'nome_completo': nome_completo,
                            'data_nascimento': data_nascimento,
                            'logradouro': logradouro_socio,
                            'numero': numero_socio,
                            'complemento': complemento_socio,
                            'bairro': bairro_socio,
                            'municipio': municipio_socio,
                            'uf': uf_socio,
                            'cep': cep_socio,
                            'telefone': telefone_socio,
                            'email': email_socio,
                            'socio_administrador': socio_administrador
                        }

                        if salvar_socio(dados_socio):
                            st.success(f"✅ Sócio {nome_completo} cadastrado com sucesso!")
                            st.balloons()
                            st.rerun()

    with tab2:
        st.markdown("### Lista de Sócios Cadastrados")

        df_socios = carregar_socios()

        if df_socios.empty:
            st.info("Nenhum sócio cadastrado ainda.")
        else:
            # Formatar CPF para exibição
            df_socios_display = df_socios.copy()
            df_socios_display['CPF'] = df_socios_display['cpf'].apply(
                lambda x: f"{x[:3]}.{x[3:6]}.{x[6:9]}-{x[9:]}" if len(str(x)) == 11 else x
            )

            # Exibir cards dos sócios
            for idx, socio in df_socios_display.iterrows():
                with st.expander(
                    f"{'⭐ ' if socio['socio_administrador'] else ''}👤 {socio['nome_completo']} - CPF: {socio['CPF']}",
                    expanded=False
                ):
                    col1, col2 = st.columns([3, 1])

                    with col1:
                        st.markdown(f"**Nome:** {socio['nome_completo']}")
                        st.markdown(f"**CPF:** {socio['CPF']}")
                        st.markdown(f"**Data de Nascimento:** {socio.get('data_nascimento', 'Não informado')}")
                        st.markdown(f"**Sócio Administrador:** {'✅ Sim' if socio['socio_administrador'] else '❌ Não'}")

                        if socio.get('logradouro'):
                            endereco = f"{socio['logradouro']}, {socio.get('numero', 's/n')}"
                            if socio.get('complemento'):
                                endereco += f" - {socio['complemento']}"
                            endereco += f" - {socio.get('bairro', '')}, {socio.get('municipio', '')}/{socio.get('uf', '')}"
                            st.markdown(f"**Endereço:** {endereco}")

                        if socio.get('telefone'):
                            st.markdown(f"**Telefone:** {socio['telefone']}")
                        if socio.get('email'):
                            st.markdown(f"**E-mail:** {socio['email']}")

                    with col2:
                        if st.button(f"🗑️ Excluir", key=f"excluir_{socio['id']}"):
                            if excluir_socio(socio['id']):
                                st.success("Sócio excluído com sucesso!")
                                st.rerun()

def tela_gerenciar_logotipos():
    """Tela para gerenciar os logotipos da empresa."""
    st.title("🖼️ Gerenciamento de Logotipos")

    # Botão voltar
    if st.button("⬅️ Voltar para Cadastro da Empresa"):
        st.session_state.tela_atual = "cadastro_empresa"
        st.rerun()

    st.markdown("---")

    # Tabs para Upload e Galeria
    tab1, tab2 = st.tabs(["📤 Upload de Logotipo", "🖼️ Galeria de Logotipos"])

    with tab1:
        st.markdown("### Fazer Upload de Novo Logotipo")
        st.info("Formatos aceitos: PNG, JPG, JPEG. Tamanho máximo: 5MB")

        with st.form("form_upload_logo", clear_on_submit=True):
            uploaded_file = st.file_uploader(
                "Selecione o arquivo de imagem",
                type=['png', 'jpg', 'jpeg'],
                help="Escolha uma imagem para o logotipo da empresa"
            )

            descricao_logo = st.text_input(
                "Descrição do Logotipo",
                placeholder="Ex: Logo Principal, Logo Sem Fundo, Logo Colorido, etc.",
                help="Digite uma descrição para identificar este logotipo"
            )

            logo_principal = st.checkbox(
                "Definir como Logotipo Principal",
                help="O logotipo principal será usado por padrão nos relatórios"
            )

            submitted = st.form_submit_button("📤 Fazer Upload", use_container_width=True, type="primary")

            if submitted:
                if not uploaded_file:
                    st.error("Selecione um arquivo de imagem!")
                elif not descricao_logo:
                    st.error("Digite uma descrição para o logotipo!")
                else:
                    # Verificar tamanho do arquivo (5MB)
                    if uploaded_file.size > 5 * 1024 * 1024:
                        st.error("Arquivo muito grande! Tamanho máximo: 5MB")
                    else:
                        # Criar diretório se não existir
                        logos_dir = 'logos_empresa'
                        if not os.path.exists(logos_dir):
                            os.makedirs(logos_dir)

                        # Gerar nome único para o arquivo
                        extensao = uploaded_file.name.split('.')[-1]
                        nome_unico = f"logo_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.{extensao}"
                        caminho_completo = os.path.join(logos_dir, nome_unico)

                        # Salvar arquivo no disco
                        with open(caminho_completo, "wb") as f:
                            f.write(uploaded_file.getbuffer())

                        # Salvar no banco de dados
                        if salvar_logotipo(uploaded_file.name, descricao_logo, caminho_completo, logo_principal):
                            st.success(f"✅ Logotipo '{descricao_logo}' enviado com sucesso!")
                            st.balloons()
                            st.rerun()

    with tab2:
        st.markdown("### Galeria de Logotipos")

        df_logos = carregar_logotipos()

        if df_logos.empty:
            st.info("Nenhum logotipo cadastrado ainda. Faça upload na aba anterior.")
        else:
            st.success(f"Total de logotipos: {len(df_logos)}")

            # Exibir logotipos em grid
            cols_per_row = 3
            for i in range(0, len(df_logos), cols_per_row):
                cols = st.columns(cols_per_row)

                for j in range(cols_per_row):
                    idx = i + j
                    if idx < len(df_logos):
                        logo = df_logos.iloc[idx]

                        with cols[j]:
                            # Card do logotipo
                            with st.container():
                                # Mostrar imagem se existir
                                if os.path.exists(logo['caminho_arquivo']):
                                    st.image(logo['caminho_arquivo'], use_container_width=True)
                                else:
                                    st.warning("Imagem não encontrada")

                                # Informações
                                if logo['logo_principal']:
                                    st.markdown("### ⭐ PRINCIPAL")
                                st.markdown(f"**{logo['descricao']}**")
                                st.caption(f"Arquivo: {logo['nome_arquivo']}")
                                st.caption(f"Upload: {logo['data_upload'][:10]}")

                                # Botões de ação
                                col_btn1, col_btn2 = st.columns(2)

                                with col_btn1:
                                    if not logo['logo_principal']:
                                        if st.button("⭐ Principal", key=f"principal_{logo['id']}", use_container_width=True):
                                            if definir_logo_principal(logo['id']):
                                                st.success("Logo principal definido!")
                                                st.rerun()

                                with col_btn2:
                                    if st.button("🗑️ Excluir", key=f"excluir_logo_{logo['id']}", use_container_width=True):
                                        if excluir_logotipo(logo['id'], logo['caminho_arquivo']):
                                            st.success("Logotipo excluído!")
                                            st.rerun()

def sidebar_botao_cadastro_empresa():
    """Exibe apenas o botão de cadastro no sidebar."""
    empresa_atual = carregar_empresa()

    st.sidebar.markdown("---")

    # Se já existe empresa cadastrada, mostra resumo
    if empresa_atual and empresa_atual.get('razao_social'):
        st.sidebar.success("✅ Empresa Cadastrada")
        st.sidebar.caption(f"**{empresa_atual.get('razao_social', 'N/A')}**")
        st.sidebar.caption(f"CNPJ: {formatar_cnpj(empresa_atual.get('cnpj', ''))}")
        botao_texto = "✏️ Editar Empresa"
    else:
        st.sidebar.warning("⚠️ Empresa não cadastrada")
        botao_texto = "➕ Cadastrar Empresa"

    if st.sidebar.button(botao_texto, use_container_width=True):
        st.session_state.tela_atual = "cadastro_empresa"
        st.rerun()

def submenu_plano_contas():
    st.subheader("1.2 Cadastro de Contas Contabeis")

    tab_csv, tab_totvs = st.tabs(["Importar CSV", "Importar TOTVS (Excel)"])

    with tab_csv:
        st.markdown("#### Importar Plano de Contas de Arquivo CSV")
        col1, col2, col3 = st.columns([2, 1, 1])
        with col1:
            uploaded_file_plano = st.file_uploader("Selecione o arquivo `plano de Contas.csv`", type=['csv'], key='upload_plano_contas')
        with col2:
            data_cadastro_importacao_str = st.text_input("Data de Cadastro (DD/MM/AAAA)", datetime.date.today().strftime('%d/%m/%Y'), key='data_csv')
        with col3:
            delimiter = st.text_input("Delimitador", value=';', max_chars=1)
        if st.button("Importar CSV", key='btn_importar_csv'):
            if uploaded_file_plano and delimiter and data_cadastro_importacao_str:
                df_importado = ler_plano_contas_csv(uploaded_file_plano, data_cadastro_importacao_str, delimiter)
                if not df_importado.empty:
                    df_existente = carregar_plano_contas()
                    codigos_existentes = df_existente['codigo'].tolist() if not df_existente.empty else []
                    contas_ja_existentes = df_importado[df_importado['codigo'].isin(codigos_existentes)]
                    contas_novas = df_importado[~df_importado['codigo'].isin(codigos_existentes)]
                    if not contas_ja_existentes.empty:
                        st.warning(f"{len(contas_ja_existentes)} contas do arquivo ja existem e foram ignoradas.")
                    if not contas_novas.empty:
                        df_final = pd.concat([df_existente, contas_novas], ignore_index=True)
                        salvar_plano_contas(df_final)
                        st.success(f"{len(contas_novas)} novas contas importadas e salvas com sucesso!")
                        st.rerun()

    with tab_totvs:
        st.markdown("#### Importar Plano de Contas do TOTVS (Excel)")
        st.info("Selecione o arquivo Excel exportado do TOTVS (planototvs.xls ou similar)")
        col1, col2 = st.columns([2, 1])
        with col1:
            uploaded_file_totvs = st.file_uploader("Selecione o arquivo Excel", type=['xls', 'xlsx'], key='upload_plano_totvs')
        with col2:
            data_cadastro_totvs = st.text_input("Data de Cadastro (DD/MM/AAAA)", datetime.date.today().strftime('%d/%m/%Y'), key='data_totvs')
        if st.button("Importar TOTVS", key='btn_importar_totvs'):
            if uploaded_file_totvs and data_cadastro_totvs:
                from data_loader import ler_plano_contas_totvs
                df_importado = ler_plano_contas_totvs(uploaded_file_totvs, data_cadastro_totvs)
                if not df_importado.empty:
                    df_existente = carregar_plano_contas()
                    codigos_existentes = df_existente['codigo'].tolist() if not df_existente.empty else []
                    contas_ja_existentes = df_importado[df_importado['codigo'].isin(codigos_existentes)]
                    contas_novas = df_importado[~df_importado['codigo'].isin(codigos_existentes)]
                    if not contas_ja_existentes.empty:
                        st.warning(f"{len(contas_ja_existentes)} contas do arquivo ja existem e foram ignoradas.")
                    if not contas_novas.empty:
                        df_final = pd.concat([df_existente, contas_novas], ignore_index=True)
                        salvar_plano_contas(df_final)
                        st.success(f"{len(contas_novas)} novas contas importadas e salvas com sucesso!")
                        st.rerun()
                    elif contas_novas.empty and not contas_ja_existentes.empty:
                        st.info("Todas as contas do arquivo ja existem no sistema.")

    st.markdown("---")

    # Abas para gerenciamento do plano de contas
    tab_visualizar, tab_nova_conta, tab_atualizar_data = st.tabs([
        "📋 Visualizar/Editar", "➕ Nova Conta", "📅 Atualizar Data Cadastro"
    ])

    with tab_visualizar:
        st.subheader("Plano de Contas Atuais")
        df_editor = carregar_plano_contas().copy()
        edited_df = st.data_editor(df_editor, num_rows="dynamic", use_container_width=True, key='editor_plano_contas')
        if st.button("💾 Salvar Alterações do Plano de Contas"):
            df_to_save = pd.DataFrame(edited_df).dropna(subset=['codigo'])
            if df_to_save['codigo'].duplicated().any():
                st.error("Erro: Existem códigos duplicados.")
            else:
                salvar_plano_contas(df_to_save)
                st.success("Plano de contas salvo com sucesso!")
                st.rerun()

    with tab_nova_conta:
        st.subheader("Cadastrar Nova Conta")
        from db_manager import inserir_conta_plano
        col1, col2 = st.columns(2)
        with col1:
            novo_codigo = st.text_input("Código (Reduzido)", key='novo_codigo')
            nova_classificacao = st.text_input("Classificação", key='nova_classificacao', placeholder="Ex: 1.1.01.001")
            nova_descricao = st.text_input("Descrição", key='nova_descricao')
        with col2:
            novo_tipo = st.selectbox("Tipo", options=['A', 'S'], index=0, key='novo_tipo',
                                    help="A = Analítica, S = Sintética")
            nova_natureza = st.selectbox("Natureza", options=['Devedora', 'Credora', 'Outra'], key='nova_natureza')
            novo_grau = st.text_input("Grau", key='novo_grau', value='1')
        nova_data_cadastro = st.text_input("Data de Cadastro (DD/MM/AAAA)", datetime.date.today().strftime('%d/%m/%Y'), key='nova_data_cadastro')

        if st.button("➕ Cadastrar Conta", key='btn_cadastrar_conta'):
            if novo_codigo and nova_classificacao and nova_descricao:
                dados = {
                    'codigo': novo_codigo,
                    'classificacao': nova_classificacao,
                    'descricao': nova_descricao,
                    'tipo': novo_tipo,
                    'natureza': nova_natureza,
                    'grau': novo_grau,
                    'data_cadastro': nova_data_cadastro,
                    'encerrada': False,
                    'data_encerramento': None
                }
                if inserir_conta_plano(dados):
                    st.success(f"Conta {novo_codigo} - {nova_descricao} cadastrada com sucesso!")
                    st.rerun()
            else:
                st.error("Preencha pelo menos Código, Classificação e Descrição.")

    with tab_atualizar_data:
        st.subheader("Atualizar Data de Cadastro em Lote")
        st.info("Use esta opção para corrigir a data de cadastro de todas as contas importadas com uma data específica.")
        from db_manager import atualizar_data_cadastro_lote

        # Mostra as datas de cadastro existentes
        df_contas = carregar_plano_contas()
        if not df_contas.empty:
            datas_unicas = df_contas['data_cadastro'].unique().tolist()
            st.write(f"**Datas de cadastro existentes:** {', '.join([str(d) for d in datas_unicas if d])}")

        col1, col2 = st.columns(2)
        with col1:
            data_antiga = st.text_input("Data Antiga (a ser substituída)", key='data_antiga', placeholder="DD/MM/AAAA")
        with col2:
            data_nova = st.text_input("Nova Data", key='data_nova', placeholder="DD/MM/AAAA")

        if st.button("🔄 Atualizar Datas", key='btn_atualizar_data'):
            if data_antiga and data_nova:
                qtd = atualizar_data_cadastro_lote(data_antiga, data_nova)
                if qtd > 0:
                    st.success(f"{qtd} contas atualizadas de {data_antiga} para {data_nova}!")
                    st.rerun()
                else:
                    st.warning(f"Nenhuma conta encontrada com a data {data_antiga}")
            else:
                st.error("Preencha ambas as datas.")


def submenu_extrato_importacao(df_bancos):
    st.subheader("2.1 Upload Extrato Bancário")

    st.info("💡 Após importar, vá em 'Menu 2.2 - Visualização' e clique em '🔄 Limpar Cache' para ver os dados atualizados!")

    tab1, tab2, tab3, tab4 = st.tabs(["📄 OFX", "📊 CSV Bradesco", "📑 PDF", "📊 Excel Daycoval"])

    with tab1:
        st.markdown("#### Upload de Arquivos OFX")
        uploaded_files = st.file_uploader("Selecione um ou mais arquivos OFX", type=['ofx', 'ofc'], accept_multiple_files=True, key='upload_ofx')
        if uploaded_files:
            # Carregar cadastro de contas ANTES para corrigir dados incompletos (ex: Bradesco)
            df_cadastro_db_atual = carregar_cadastro_contas()
            df_ofx = importar_multiplos_extratos(uploaded_files, df_cadastro=df_cadastro_db_atual)
            if not df_ofx.empty:
                salvar_contas_ofx_faltantes(df_ofx, df_cadastro_db_atual, df_bancos)
                salvar_extrato_bancario_historico(df_ofx)
                st.success(f"✅ Transações salvas com sucesso no histórico.")
                st.rerun()

    with tab2:
        st.markdown("#### Upload de Arquivos CSV do Bradesco")
        st.info("Formato esperado: CSV exportado pelo Bradesco com colunas Data, Lançamento, Dcto., Crédito (R$), Débito (R$), Saldo (R$)")
        uploaded_csv_files = st.file_uploader("Selecione um ou mais arquivos CSV do Bradesco", type=['csv'], accept_multiple_files=True, key='upload_csv_bradesco')
        if uploaded_csv_files:
            from data_loader import importar_multiplos_csvs_bradesco
            df_csv = importar_multiplos_csvs_bradesco(uploaded_csv_files)
            if not df_csv.empty:
                df_cadastro_db_atual = carregar_cadastro_contas()
                salvar_contas_ofx_faltantes(df_csv, df_cadastro_db_atual, df_bancos)
                salvar_extrato_bancario_historico(df_csv)
                st.success(f"✅ {len(df_csv)} transações CSV salvas com sucesso no histórico.")
                st.rerun()

    with tab3:
        st.markdown("#### Upload de Arquivos PDF")
        st.info("Suportado: Extratos em PDF do Sicredi")
        uploaded_pdf_files = st.file_uploader("Selecione um ou mais arquivos PDF", type=['pdf'], accept_multiple_files=True, key='upload_pdf')
        if uploaded_pdf_files:
            df_cadastro_db_atual = carregar_cadastro_contas()
            df_pdf = importar_multiplos_extratos(uploaded_pdf_files, df_cadastro=df_cadastro_db_atual)
            if not df_pdf.empty:
                salvar_contas_ofx_faltantes(df_pdf, df_cadastro_db_atual, df_bancos)
                salvar_extrato_bancario_historico(df_pdf)
                st.success(f"✅ {len(df_pdf)} transações PDF salvas com sucesso no histórico.")
                st.rerun()

    with tab4:
        st.markdown("#### Upload de Arquivos Excel do Daycoval")
        st.info("Suportado: Arquivos .xls ou .xlsx exportados do Banco Daycoval")
        uploaded_excel_files = st.file_uploader("Selecione um ou mais arquivos Excel", type=['xls', 'xlsx'], accept_multiple_files=True, key='upload_excel_daycoval')
        if uploaded_excel_files:
            df_cadastro_db_atual = carregar_cadastro_contas()
            df_excel = importar_multiplos_extratos(uploaded_excel_files, df_cadastro=df_cadastro_db_atual)
            if not df_excel.empty:
                salvar_contas_ofx_faltantes(df_excel, df_cadastro_db_atual, df_bancos)
                salvar_extrato_bancario_historico(df_excel)
                st.success(f"✅ {len(df_excel)} transações Excel salvas com sucesso no histórico.")
                st.rerun()

def submenu_extrato_visualizacao():
    st.subheader("2.2 📖 Visualização de Extrato Salvo (Histórico) e Saldo")

    # Botão para limpar cache
    col_btn1, col_btn2 = st.columns([1, 4])
    with col_btn1:
        if st.button("🔄 Limpar Cache", help="Limpa o cache e atualiza os dados da visualização"):
            st.cache_data.clear()
            st.success("Cache limpo com sucesso! Recarregue os dados.")
            st.rerun()
    with col_btn2:
        st.caption("💡 Use este botão se os dados não estiverem atualizados após importar novas transações")

    st.markdown("---")

    df_contas = carregar_cadastro_contas()
    if df_contas.empty:
        st.warning("O Cadastro de Contas (Menu 1) está vazio.")
        return

    # Corrigido: Criar a coluna 'Display' diretamente no DataFrame principal
    df_contas['Display'] = df_contas['Agencia'].astype(str) + " / " + df_contas['Conta'].astype(str)
    
    # Usar uma view sem duplicatas para o selectbox, mas o df_contas original para buscar dados
    contas_display = df_contas[['Display']].drop_duplicates().sort_values('Display')

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        conta_selecionada_display = st.selectbox("Selecione a Conta Bancária:", options=contas_display['Display'].tolist())
    
    # Filtrar o DataFrame original que contém todos os dados
    conta_selecionada_row = df_contas[df_contas['Display'] == conta_selecionada_display].iloc[0]
    conta_ofx_normalizada = conta_selecionada_row['Conta_OFX_Normalizada']
    saldo_inicial_cadastro = conta_selecionada_row.get('Saldo Inicial', 0.0)
    data_inicial_saldo = conta_selecionada_row.get('Data Inicial Saldo', None)

    today = datetime.date.today()
    last_month = today - datetime.timedelta(days=30)

    with col2:
        data_inicio_str = st.text_input("Data de Início (DD/MM/AAAA)", value=last_month.strftime('%d/%m/%Y'))
    with col3:
        data_fim_str = st.text_input("Data Final (DD/MM/AAAA)", value=today.strftime('%d/%m/%Y'))

    if st.button("🔍 Carregar Extrato"):
        try:
            data_inicio = datetime.datetime.strptime(data_inicio_str, '%d/%m/%Y').date()
            data_fim = datetime.datetime.strptime(data_fim_str, '%d/%m/%Y').date()

            # Calcula o saldo inicial REAL para o período consultado
            # = Saldo do cadastro + todas as transações até o dia anterior
            saldo_inicial_real = saldo_inicial_cadastro

            # Se não há data inicial de saldo no cadastro, buscar a transação mais antiga
            if not data_inicial_saldo or pd.isna(data_inicial_saldo):
                # Buscar todas as transações desta conta para encontrar a data mais antiga
                df_todas = carregar_extrato_bancario_historico(conta_ofx_normalizada, datetime.date(2000, 1, 1), datetime.date.today())
                if not df_todas.empty and 'Data Lançamento' in df_todas.columns:
                    data_inicial_saldo = df_todas['Data Lançamento'].min()
                    st.info(f"ℹ️ 'Data Inicial Saldo' não está preenchida no cadastro. Usando a data da transação mais antiga ({data_inicial_saldo.strftime('%d/%m/%Y')}) como referência. Para alterar, edite o cadastro no Menu 1.1.")
                else:
                    data_inicial_saldo = None

            if data_inicial_saldo:
                # Converte data inicial se for string
                if isinstance(data_inicial_saldo, str):
                    try:
                        data_inicial_saldo = datetime.datetime.strptime(data_inicial_saldo, '%Y-%m-%d').date()
                    except:
                        try:
                            data_inicial_saldo = datetime.datetime.strptime(data_inicial_saldo, '%d/%m/%Y').date()
                        except:
                            data_inicial_saldo = None

                if data_inicial_saldo and data_inicial_saldo < data_inicio:
                    # Busca todas as transações desde a data inicial do saldo até um dia antes do período
                    data_ate_antes = data_inicio - datetime.timedelta(days=1)
                    df_antes = carregar_extrato_bancario_historico(conta_ofx_normalizada, data_inicial_saldo, data_ate_antes)

                    if not df_antes.empty:
                        saldo_acumulado_antes = df_antes['Valor'].sum()
                        saldo_inicial_real = saldo_inicial_cadastro + saldo_acumulado_antes
                        st.success(f"✅ Saldo inicial calculado: R$ {saldo_inicial_cadastro:,.2f} (cadastro em {data_inicial_saldo.strftime('%d/%m/%Y')}) + R$ {saldo_acumulado_antes:,.2f} (movimentações até {data_ate_antes.strftime('%d/%m/%Y')}) = R$ {saldo_inicial_real:,.2f}")

            df_historico = carregar_extrato_bancario_historico(conta_ofx_normalizada, data_inicio, data_fim)

            if not df_historico.empty:
                st.dataframe(df_historico, width='stretch')

                saldo_periodo = df_historico['Valor'].sum()
                total_entradas = df_historico[df_historico['Valor'] > 0]['Valor'].sum()
                total_saidas = df_historico[df_historico['Valor'] < 0]['Valor'].sum()
                saldo_final = saldo_inicial_real + saldo_periodo

                st.markdown("---")
                st.subheader("Totalizadores do Período")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Saldo Inicial", f"R$ {saldo_inicial_real:,.2f}")
                col2.metric("Total de Entradas", f"R$ {total_entradas:,.2f}")
                col3.metric("Total de Saídas", f"R$ {total_saidas:,.2f}")
                col4.metric("Saldo Final", f"R$ {saldo_final:,.2f}")
            else:
                st.info("Nenhum registro encontrado para o período e conta selecionados.")

        except ValueError:
            st.error("Formato de data inválido. Por favor, use DD/MM/AAAA.")

def submenu_lancamentos_contabeis_visualizacao():
    st.subheader("4.0 Visualizar Lançamentos")
    
    st.markdown("##### Filtros")
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        data_inicio_str = st.text_input("Data Inicial (DD/MM/YYYY)", key="filtro_data_inicio")
    with col_f2:
        data_fim_str = st.text_input("Data Final (DD/MM/YYYY)", key="filtro_data_fim")
    with col_f3:
        conta_reduzida_filtro = st.text_input("Conta Reduzida", key="filtro_conta_reduzida")

    # Inicializa o estado para armazenar os dados filtrados
    if 'df_lancamentos_filtrados' not in st.session_state:
        st.session_state.df_lancamentos_filtrados = pd.DataFrame()

    if st.button("Buscar"):
        all_transactions = carregar_lancamentos_contabeis().copy()
        filtro_limpo = conta_reduzida_filtro.strip()
        
        df_display = all_transactions.copy()
        df_display['data_lancamento_obj'] = pd.to_datetime(df_display['data_lancamento'], errors='coerce').dt.date

        try:
            if data_inicio_str:
                data_inicio_filtro = datetime.datetime.strptime(data_inicio_str, "%d/%m/%Y").date()
                df_display = df_display[df_display['data_lancamento_obj'] >= data_inicio_filtro]
            if data_fim_str:
                data_fim_filtro = datetime.datetime.strptime(data_fim_str, "%d/%m/%Y").date()
                df_display = df_display[df_display['data_lancamento_obj'] <= data_fim_filtro]
        except ValueError:
            st.error("Formato de data inválido. Use DD/MM/YYYY.")
            df_display = pd.DataFrame() # Limpa em caso de erro

        if filtro_limpo and not df_display.empty:
            col_deb_str = pd.to_numeric(df_display['reduz_deb'], errors='coerce').astype('Int64').astype(str)
            col_cred_str = pd.to_numeric(df_display['reduz_cred'], errors='coerce').astype('Int64').astype(str)
            df_display = df_display[(col_deb_str == filtro_limpo) | (col_cred_str == filtro_limpo)]
        
        # Armazena o resultado no session_state
        st.session_state.df_lancamentos_filtrados = df_display
        
        # Limpa estados de confirmação de exclusão de buscas anteriores
        st.session_state.confirm_delete_selected = False
        st.session_state.confirm_delete_all_filtered = False
        st.rerun()

    # --- LÓGICA DE EXIBIÇÃO E AÇÕES (FORA DO BOTÃO BUSCAR) ---
    df_display = st.session_state.df_lancamentos_filtrados

    if not df_display.empty:
        # --- PREPARAÇÃO DO DATAFRAME PARA EXIBIÇÃO ---
        df_para_exibir = df_display.copy()
        df_para_exibir['data_lancamento'] = pd.to_datetime(df_para_exibir['data_lancamento_obj']).dt.strftime('%d/%m/%Y')
        df_para_exibir.loc[:, 'reduz_deb'] = pd.to_numeric(df_para_exibir['reduz_deb'], errors='coerce').astype('Int64')
        df_para_exibir.loc[:, 'reduz_cred'] = pd.to_numeric(df_para_exibir['reduz_cred'], errors='coerce').astype('Int64')
        
        df_para_exibir.insert(0, "Selecionar", False)

        colunas_base = ['id', 'data_lancamento', 'idlancamento', 'reduz_deb', 'nome_conta_d', 'reduz_cred', 'nome_conta_c', 'valor', 'historico', 'tipo_lancamento', 'origem']
        colunas_ordenadas = ["Selecionar"] + colunas_base
        colunas_desabilitadas = colunas_base

        edited_df = st.data_editor(
            df_para_exibir[colunas_ordenadas],
            width='stretch',
            hide_index=True,
            column_order=colunas_ordenadas,
            disabled=colunas_desabilitadas,
            key="lancamentos_editor"
        )

        # --- LÓGICA DE EXCLUSÃO ---
        linhas_selecionadas = edited_df[edited_df['Selecionar']]
        ids_selecionados = linhas_selecionadas['id'].tolist()
        todos_ids_filtrados = df_display['id'].tolist()

        st.markdown("---")
        st.markdown("##### Ações de Exclusão")
        col1, col2 = st.columns(2)

        with col1:
            if not ids_selecionados:
                st.button("🗑️ Excluir Selecionados", disabled=True, help="Marque a caixa de seleção de um ou mais lançamentos para habilitar.")
            else:
                if st.button(f"🗑️ Excluir {len(ids_selecionados)} Lançamento(s) Selecionado(s)"):
                    st.session_state['confirm_delete_all_filtered'] = False
                    if st.session_state.get('confirm_delete_selected', False):
                        excluir_lancamentos_por_ids(ids_selecionados)
                        st.session_state.df_lancamentos_filtrados = df_display[~df_display['id'].isin(ids_selecionados)]
                        st.session_state['confirm_delete_selected'] = False
                        st.success("Lançamento(s) excluído(s) com sucesso!")
                        st.rerun()
                    else:
                        st.session_state['confirm_delete_selected'] = True
                        st.rerun()
                if st.session_state.get('confirm_delete_selected'):
                     st.warning("Clique novamente para confirmar a exclusão dos lançamentos SELECIONADOS.")

        with col2:
            if st.button(f"🔥 Excluir TODOS os {len(todos_ids_filtrados)} Lançamentos Filtrados"):
                st.session_state['confirm_delete_selected'] = False
                if st.session_state.get('confirm_delete_all_filtered', False):
                    excluir_lancamentos_por_ids(todos_ids_filtrados)
                    st.session_state.df_lancamentos_filtrados = pd.DataFrame() # Limpa o DF
                    st.session_state['confirm_delete_all_filtered'] = False
                    st.success("Todos os lançamentos filtrados foram excluídos com sucesso!")
                    st.rerun()
                else:
                    st.session_state['confirm_delete_all_filtered'] = True
                    st.rerun()
            if st.session_state.get('confirm_delete_all_filtered'):
                st.warning("Clique novamente para confirmar a exclusão de TODOS os lançamentos visíveis.")

        if not ids_selecionados:
            st.session_state['confirm_delete_selected'] = False
        
        # --- TOTALIZADORES ---
        st.markdown("---")
        st.subheader("Totalizadores")
        filtro_limpo = conta_reduzida_filtro.strip()
        if filtro_limpo:
            # Recalcular saldo inicial com base no DF original antes de qualquer filtro de data
            saldo_inicial = 0
            if data_inicio_str:
                try:
                    data_inicio_filtro = datetime.datetime.strptime(data_inicio_str, "%d/%m/%Y").date()
                    df_saldo_inicial = carregar_lancamentos_contabeis()
                    df_saldo_inicial = df_saldo_inicial[pd.to_datetime(df_saldo_inicial['data_lancamento']).dt.date < data_inicio_filtro]
                    
                    col_deb_hist = pd.to_numeric(df_saldo_inicial['reduz_deb'], errors='coerce').astype('Int64').astype(str)
                    col_cred_hist = pd.to_numeric(df_saldo_inicial['reduz_cred'], errors='coerce').astype('Int64').astype(str)

                    debitos_passados = df_saldo_inicial.loc[col_deb_hist == filtro_limpo, 'valor'].sum()
                    creditos_passados = df_saldo_inicial.loc[col_cred_hist == filtro_limpo, 'valor'].sum()
                    saldo_inicial = creditos_passados - debitos_passados
                except ValueError:
                    st.error("Formato de data inválido para cálculo do Saldo Inicial.")

            col_deb_periodo = pd.to_numeric(df_display['reduz_deb'], errors='coerce').astype('Int64').astype(str)
            col_cred_periodo = pd.to_numeric(df_display['reduz_cred'], errors='coerce').astype('Int64').astype(str)

            total_debito = df_display.loc[col_deb_periodo == filtro_limpo, 'valor'].sum()
            total_credito = df_display.loc[col_cred_periodo == filtro_limpo, 'valor'].sum()
            saldo_final = saldo_inicial + total_credito - total_debito

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Saldo Inicial", f"R$ {saldo_inicial:,.2f}")
            col2.metric("Total Crédito", f"R$ {total_credito:,.2f}")
            col3.metric("Total Débito", f"R$ {total_debito:,.2f}")
            col4.metric("Saldo Final", f"R$ {saldo_final:,.2f}")
        else:
            total_geral = df_display['valor'].sum()
            col1, col2 = st.columns(2)
            col1.metric("Total Débito (Geral)", f"R$ {total_geral:,.2f}")
            col2.metric("Total Crédito (Geral)", f"R$ {total_geral:,.2f}")
    else:
        # Verifica se o botão de busca já foi pressionado para diferenciar estado inicial de busca sem resultados
        if 'df_lancamentos_filtrados' in st.session_state and st.session_state.df_lancamentos_filtrados is not None:
             st.info("Nenhum lançamento encontrado com os filtros aplicados.")
        else:
             st.info("Use os filtros e clique em 'Buscar' para ver os lançamentos.")

def submenu_lancamentos_contabeis_adicionar():
    st.subheader("4.1 Adicionar Lançamento Manual")
    
    plano_contas_df = carregar_plano_contas()

    # Inicializar estado da sessão se necessário
    if 'debit_entries' not in st.session_state:
        st.session_state.debit_entries = [{'id': uuid.uuid4(), 'conta': '', 'nome': '', 'valor': 0.0}]
    if 'credit_entries' not in st.session_state:
        st.session_state.credit_entries = [{'id': uuid.uuid4(), 'conta': '', 'nome': '', 'valor': 0.0}]

    # --- Lógica de atualização de nomes ---
    for entry in st.session_state.debit_entries + st.session_state.credit_entries:
        if entry['conta']:
            match = plano_contas_df[plano_contas_df['codigo'] == str(entry['conta'])]
            entry['nome'] = match.iloc[0]['descricao'] if not match.empty else "Inválida"

    # --- Layout da UI ---
    col1, col2, col3 = st.columns(3)
    with col1:
        data_lancamento_str = st.text_input("Data (DD/MM/AAAA)", datetime.date.today().strftime('%d/%m/%Y'))
    with col2:
        tipo_lancamento = st.selectbox("Tipo de Lançamento", ["Inclusão", "Baixa"])
    with col3:
        tipo_de_partida = st.selectbox(
            "Tipo de Partida",
            ["Um Débito para Um Crédito", "Um Débito para Vários Créditos", "Vários Débitos para Um Crédito", "Vários Débitos para Vários Créditos"],
            key='tipo_de_partida'
        )
    
    historico = st.text_area("Histórico do Lançamento")
    st.markdown("---")

    # --- Renderização Manual das Partidas ---
    col_deb, col_cred = st.columns(2)

    with col_deb:
        st.markdown("##### Partidas a Débito")
        for i, entry in enumerate(st.session_state.debit_entries):
            cols = st.columns([2, 4, 3])
            entry['conta'] = cols[0].text_input("Conta Reduzida", value=entry['conta'], key=f"deb_conta_input_{entry['id']}")
            entry['nome'] = cols[1].text_input("Nome da Conta", value=entry['nome'], key=f"deb_nome_display_{entry['id']}", disabled=True)
            entry['valor'] = cols[2].number_input("Valor", value=entry['valor'], key=f"deb_valor_{entry['id']}", format="%.2f")
        
        if "Vários" in tipo_de_partida:
            if st.button("Adicionar Débito", key="add_debito"):
                st.session_state.debit_entries.append({'id': uuid.uuid4(), 'conta': '', 'nome': '', 'valor': 0.0})
                st.rerun()

    with col_cred:
        st.markdown("##### Partidas a Crédito")
        for i, entry in enumerate(st.session_state.credit_entries):
            cols = st.columns([2, 4, 3])
            entry['conta'] = cols[0].text_input("Conta Reduzida", value=entry['conta'], key=f"cred_conta_input_{entry['id']}")
            entry['nome'] = cols[1].text_input("Nome da Conta", value=entry['nome'], key=f"cred_nome_display_{entry['id']}", disabled=True)
            entry['valor'] = cols[2].number_input("Valor", value=entry['valor'], key=f"cred_valor_{entry['id']}", format="%.2f")

        if "Vários" in tipo_de_partida:
            if st.button("Adicionar Crédito", key="add_credito"):
                st.session_state.credit_entries.append({'id': uuid.uuid4(), 'conta': '', 'nome': '', 'valor': 0.0})
                st.rerun()

    # Ajustar número de linhas com base no tipo de partida
    if "Um Débito" in tipo_de_partida and len(st.session_state.debit_entries) > 1:
        st.session_state.debit_entries = st.session_state.debit_entries[:1]
        st.rerun()
    if "Um Crédito" in tipo_de_partida and len(st.session_state.credit_entries) > 1:
        st.session_state.credit_entries = st.session_state.credit_entries[:1]
        st.rerun()

    # --- Totalizadores e Botões de Ação ---
    st.markdown("---")
    total_debito = sum(e['valor'] for e in st.session_state.debit_entries)
    total_credito = sum(e['valor'] for e in st.session_state.credit_entries)
    diferenca = total_debito - total_credito

    col_tot1, col_tot2, col_tot3 = st.columns(3)
    col_tot1.metric("Total Débito", f"R$ {total_debito:,.2f}")
    col_tot2.metric("Total Crédito", f"R$ {total_credito:,.2f}")
    col_tot3.metric("Diferença", f"R$ {diferenca:,.2f}", delta_color="off" if diferenca == 0 else "inverse")

    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("Salvar Lançamento", disabled=(diferenca != 0 or total_debito == 0), width='stretch'):
            partidas_para_salvar = []
            idlanc = str(uuid.uuid4()) # Gerar um UUID para o idlancamento manual
            
            # st.write(f"ID do Lançamento (idlanc): {idlanc}") # Debug (removido ou comentado) 
            
            try:
                data_lanc = datetime.datetime.strptime(data_lancamento_str, '%d/%m/%Y').strftime('%Y-%m-%d')
                
                # Re-consultar os nomes das contas no momento do salvamento para garantir que estejam atualizados
                final_debits = []
                for d_entry in st.session_state.debit_entries:
                    if d_entry.get("conta") and d_entry.get("valor"):
                        match = plano_contas_df[plano_contas_df['codigo'] == str(d_entry['conta'])]
                        nome_conta = match.iloc[0]['descricao'] if not match.empty else "Inválida"
                        final_debits.append({'conta': d_entry['conta'], 'nome': nome_conta, 'valor': d_entry['valor']})

                final_credits = []
                for c_entry in st.session_state.credit_entries:
                    if c_entry.get("conta") and c_entry.get("valor"):
                        match = plano_contas_df[plano_contas_df['codigo'] == str(c_entry['conta'])]
                        nome_conta = match.iloc[0]['descricao'] if not match.empty else "Inválida"
                        final_credits.append({'conta': c_entry['conta'], 'nome': nome_conta, 'valor': c_entry['valor']})

                if not final_debits or not final_credits:
                    st.error("É necessário preencher pelo menos uma partida de débito e uma de crédito com valor.")
                    return

                partida_tipo = st.session_state.tipo_de_partida
                
                if partida_tipo == "Um Débito para Um Crédito":
                    partidas_para_salvar.append({
                        'idlancamento': idlanc, 'data_lancamento': data_lanc, 'historico': historico, 'valor': final_debits[0]['valor'],
                        'tipo_lancamento': tipo_lancamento, 'reduz_deb': final_debits[0]['conta'], 'nome_conta_d': final_debits[0]['nome'],
                        'reduz_cred': final_credits[0]['conta'], 'nome_conta_c': final_credits[0]['nome'], 'origem': 'Manual'
                    })
                elif partida_tipo == "Um Débito para Vários Créditos":
                    deb_acc = final_debits[0]
                    for cred_acc in final_credits:
                        partidas_para_salvar.append({
                            'idlancamento': idlanc, 'data_lancamento': data_lanc, 'historico': historico, 'valor': cred_acc['valor'],
                            'tipo_lancamento': tipo_lancamento, 'reduz_deb': deb_acc['conta'], 'nome_conta_d': deb_acc['nome'],
                            'reduz_cred': cred_acc['conta'], 'nome_conta_c': cred_acc['nome'], 'origem': 'Manual'
                        })
                elif partida_tipo == "Vários Débitos para Um Crédito":
                    cred_acc = final_credits[0]
                    for deb_acc in final_debits:
                        partidas_para_salvar.append({
                            'idlancamento': idlanc, 'data_lancamento': data_lanc, 'historico': historico, 'valor': deb_acc['valor'],
                            'tipo_lancamento': tipo_lancamento, 'reduz_deb': deb_acc['conta'], 'nome_conta_d': deb_acc['nome'],
                            'reduz_cred': cred_acc['conta'], 'nome_conta_c': cred_acc['nome'], 'origem': 'Manual'
                        })
                elif partida_tipo == "Vários Débitos para Vários Créditos":
                    for deb_entry in final_debits:
                        partidas_para_salvar.append({
                            'idlancamento': idlanc, 'data_lancamento': data_lanc, 'historico': historico,
                            'valor': deb_entry['valor'], 'tipo_lancamento': tipo_lancamento,
                            'reduz_deb': deb_entry['conta'], 'nome_conta_d': deb_entry['nome'],
                            'reduz_cred': None, 'nome_conta_c': None, 'origem': 'Manual'
                        })
                    for cred_entry in final_credits:
                        partidas_para_salvar.append({
                            'idlancamento': idlanc, 'data_lancamento': data_lanc, 'historico': historico,
                            'valor': cred_entry['valor'], 'tipo_lancamento': tipo_lancamento,
                            'reduz_deb': None, 'nome_conta_d': None,
                            'reduz_cred': cred_entry['conta'], 'nome_conta_c': cred_entry['nome'], 'origem': 'Manual'
                        })
                

                try:
                    if salvar_partidas_lancamento(partidas_para_salvar):
                        st.success("Lançamento salvo com sucesso!")
                        st.session_state.debit_entries = [{'id': uuid.uuid4(), 'conta': '', 'nome': '', 'valor': 0.0}]
                        st.session_state.credit_entries = [{'id': uuid.uuid4(), 'conta': '', 'nome': '', 'valor': 0.0}]
                        st.rerun()
                    else:
                        st.error("Ocorreu um erro ao salvar o lançamento no banco de dados.")
                except Exception as db_e:
                    st.error(f"Erro ao salvar no banco de dados: {db_e}")

            except ValueError:
                st.error("Formato de data inválido. Use DD/MM/AAAA.")
            except Exception as e:
                st.error(f"Ocorreu um erro inesperado: {e}")
    
    with col_btn2:
        if st.button("Limpar Lançamento", width='stretch'):
            st.session_state.debit_entries = [{'id': uuid.uuid4(), 'conta': '', 'nome': '', 'valor': 0.0}]
            st.session_state.credit_entries = [{'id': uuid.uuid4(), 'conta': '', 'nome': '', 'valor': 0.0}]
            st.rerun()

def submenu_relatorios_extratos_bancarios():
    st.subheader("6.1 Extratos Bancários")

    st.markdown("""
    Gera um relatório de extrato bancário em formato PDF, replicando o modelo oficial do banco.
    Selecione a conta, o período e clique em gerar para baixar o PDF.
    """)

    # Seleção de Conta
    df_contas = carregar_cadastro_contas()
    if df_contas.empty:
        st.warning("O Cadastro de Contas (Menu 1.1) está vazio. É necessário cadastrar as contas primeiro.")
        return

    df_contas['Display'] = df_contas['Agencia'].astype(str) + " / " + df_contas['Conta'].astype(str)
    contas_display = df_contas[['Display', 'Codigo_Banco']].drop_duplicates().sort_values('Display')

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        conta_selecionada_display = st.selectbox(
            "Selecione a Conta Bancária:",
            options=contas_display['Display'].tolist(),
            key="rel_extrato_conta"
        )

    today = datetime.date.today()
    last_month = today - datetime.timedelta(days=30)

    with col2:
        data_inicio_str = st.text_input(
            "Data de Início (DD/MM/AAAA)",
            value=last_month.strftime('%d/%m/%Y'),
            key="rel_extrato_data_inicio"
        )
    with col3:
        data_fim_str = st.text_input(
            "Data Final (DD/MM/AAAA)",
            value=today.strftime('%d/%m/%Y'),
            key="rel_extrato_data_fim"
        )

    if st.button("📄 Gerar Visualização do Extrato"):
        try:
            data_inicio = datetime.datetime.strptime(data_inicio_str, '%d/%m/%Y').date()
            data_fim = datetime.datetime.strptime(data_fim_str, '%d/%m/%Y').date()

            # Carregar dados da empresa
            empresa_info = carregar_empresa()
            if not empresa_info or not empresa_info.get('razao_social'):
                st.error("Cadastre os dados da empresa primeiro (Sidebar > Cadastrar Empresa)")
                return

            # Buscar informações da conta
            conta_selecionada_row = df_contas[df_contas['Display'] == conta_selecionada_display].iloc[0]
            conta_ofx_normalizada = conta_selecionada_row['Conta_OFX_Normalizada']

            # Carregar extrato do período
            df_extrato = carregar_extrato_bancario_historico(conta_ofx_normalizada, data_inicio, data_fim)

            if df_extrato.empty:
                st.warning("Nenhuma transação encontrada para o período selecionado.")
                return

            # Calcular saldo inicial real (saldo cadastrado + movimentações antes do período)
            saldo_cadastrado = conta_selecionada_row.get('Saldo Inicial', 0.0)

            # Buscar todas as transações anteriores à data inicial
            data_cadastro = conta_selecionada_row.get('Data Inicial Saldo')
            if data_cadastro and pd.notna(data_cadastro):
                try:
                    data_inicial_cadastro = pd.to_datetime(data_cadastro, format='%d/%m/%Y').date()
                except:
                    data_inicial_cadastro = datetime.date(2000, 1, 1)
            else:
                data_inicial_cadastro = datetime.date(2000, 1, 1)

            # Buscar movimentações entre a data de cadastro e o dia anterior ao período
            data_anterior = data_inicio - datetime.timedelta(days=1)
            df_anterior = carregar_extrato_bancario_historico(conta_ofx_normalizada, data_inicial_cadastro, data_anterior)

            saldo_movimentacoes_anteriores = df_anterior['Valor'].sum() if not df_anterior.empty else 0.0
            saldo_inicial_real = saldo_cadastrado + saldo_movimentacoes_anteriores

            # SALVAR NO SESSION STATE
            st.session_state.extrato_preview = {
                'df_extrato': df_extrato,
                'empresa_info': empresa_info,
                'conta_row': conta_selecionada_row,
                'saldo_inicial': saldo_inicial_real,
                'data_inicio': data_inicio,
                'data_fim': data_fim,
                'data_inicio_str': data_inicio_str,
                'data_fim_str': data_fim_str,
                'conta_ofx_normalizada': conta_ofx_normalizada
            }

        except ValueError:
            st.error("Formato de data inválido. Por favor, use DD/MM/AAAA.")
        except Exception as e:
            st.error(f"Ocorreu um erro ao gerar o extrato: {e}")

    # VERIFICAR SE HÁ DADOS SALVOS NO SESSION STATE PARA EXIBIR
    if 'extrato_preview' in st.session_state:
        preview_data = st.session_state.extrato_preview
        df_extrato = preview_data['df_extrato']
        empresa_info = preview_data['empresa_info']
        conta_selecionada_row = preview_data['conta_row']
        saldo_inicial_real = preview_data['saldo_inicial']
        data_inicio = preview_data['data_inicio']
        data_fim = preview_data['data_fim']
        data_inicio_str = preview_data['data_inicio_str']
        data_fim_str = preview_data['data_fim_str']
        conta_ofx_normalizada = preview_data['conta_ofx_normalizada']

        # VISUALIZAÇÃO PRÉVIA
        st.markdown("---")
        st.markdown("### 📋 Visualização do Extrato")

        # Informações do cabeçalho
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info(f"**Empresa:** {empresa_info['razao_social']}")
        with col2:
            st.info(f"**Agência:** {conta_selecionada_row['Agencia']}")
        with col3:
            st.info(f"**Conta:** {conta_selecionada_row['Conta']}")

        # Saldos
        col1, col2, col3 = st.columns(3)
        total_entradas = df_extrato[df_extrato['Valor'] > 0]['Valor'].sum()
        total_saidas = abs(df_extrato[df_extrato['Valor'] < 0]['Valor'].sum())
        saldo_final = saldo_inicial_real + df_extrato['Valor'].sum()

        with col1:
            st.metric("Saldo Inicial", f"R$ {saldo_inicial_real:,.2f}")
        with col2:
            st.metric("Total Entradas", f"R$ {total_entradas:,.2f}", delta=None, delta_color="normal")
        with col3:
            st.metric("Total Saídas", f"R$ {total_saidas:,.2f}", delta=None, delta_color="inverse")

        st.metric("**Saldo Final**", f"R$ {saldo_final:,.2f}")

        # Tabela de lançamentos
        st.markdown("#### Lançamentos do Período")
        df_display = df_extrato.copy()
        df_display['Data Lançamento'] = pd.to_datetime(df_display['Data Lançamento']).dt.strftime('%d/%m/%Y')
        df_display['Valor Formatado'] = df_display['Valor'].apply(
            lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        )

        st.dataframe(
            df_display[['Data Lançamento', 'Descrição', 'Tipo', 'Valor Formatado']],
            use_container_width=True,
            hide_index=True
        )

        st.markdown("---")

        # Botão para gerar PDF
        if st.button("📄 Gerar PDF do Extrato", type="primary", use_container_width=True, key="btn_gerar_pdf_extrato"):
            # Preparar informações da conta para o PDF
            info_conta = {
                'Associado': empresa_info['razao_social'],
                'Cooperativa': conta_selecionada_row['Agencia'],
                'Conta': conta_selecionada_row['Conta'],
                'Codigo_Banco': conta_selecionada_row['Codigo_Banco'],
                'Path_Logo': obter_logo_principal(),
                'Saldo Inicial': saldo_inicial_real
            }

            # Gerar PDF
            with st.spinner("Gerando extrato em PDF..."):
                pdf_buffer = gerar_extrato_bancario_pdf(df_extrato, info_conta, data_inicio, data_fim)

            # Botão de download
            nome_arquivo = f"extrato_{conta_selecionada_row['Codigo_Banco']}_{conta_ofx_normalizada}_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"

            st.success("✅ Extrato gerado com sucesso!")
            st.download_button(
                label="⬇️ Baixar Extrato PDF",
                data=pdf_buffer,
                file_name=nome_arquivo,
                mime="application/pdf",
                key="download_extrato_pdf"
            )

def submenu_relatorio_balancete():
    st.subheader("6.2 Balancete de Verificação")

    st.markdown("""
    Gera o Balancete de Verificação mostrando débitos, créditos e saldos de todas as contas contábeis.
    """)

    # Carregar dados da empresa
    empresa_info = carregar_empresa()
    if not empresa_info:
        st.warning("Cadastre os dados da empresa primeiro (Menu Sidebar > Cadastrar Empresa).")
        return

    # Obter logo principal
    logo_path = obter_logo_principal()

    # Período
    col1, col2 = st.columns(2)
    today = datetime.date.today()
    first_day = today.replace(day=1)

    with col1:
        data_inicio_str = st.text_input("Data Inicial (DD/MM/AAAA)", value=first_day.strftime('%d/%m/%Y'))
    with col2:
        data_fim_str = st.text_input("Data Final (DD/MM/AAAA)", value=today.strftime('%d/%m/%Y'))

    if st.button("📊 Gerar Visualização do Balancete"):
        try:
            data_inicio = datetime.datetime.strptime(data_inicio_str, '%d/%m/%Y').date()
            data_fim = datetime.datetime.strptime(data_fim_str, '%d/%m/%Y').date()

            df_lancamentos = carregar_lancamentos_contabeis()
            df_plano_contas = carregar_plano_contas()

            if df_lancamentos.empty:
                st.warning("Nenhum lançamento contábil encontrado.")
                return

            # Converter data_lancamento para datetime se for string
            if df_lancamentos['data_lancamento'].dtype == 'object':
                df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'])

            # Filtrar lançamentos do período
            df_periodo = df_lancamentos[
                (pd.to_datetime(df_lancamentos['data_lancamento']).dt.date >= data_inicio) &
                (pd.to_datetime(df_lancamentos['data_lancamento']).dt.date <= data_fim)
            ].copy()

            # Normalizar códigos de conta (remover .0 se for float)
            df_periodo['reduz_deb'] = df_periodo['reduz_deb'].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
            )
            df_periodo['reduz_cred'] = df_periodo['reduz_cred'].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
            )

            # Calcular totais por conta
            balancete = []
            contas_deb = df_periodo['reduz_deb'].dropna().unique()
            contas_cred = df_periodo['reduz_cred'].dropna().unique()
            todas_contas = set(list(contas_deb) + list(contas_cred))

            for conta in sorted(todas_contas, key=lambda x: str(x)):
                conta_str = str(conta)
                conta_info = df_plano_contas[df_plano_contas['codigo'] == conta_str]
                nome_conta = conta_info.iloc[0]['descricao'] if not conta_info.empty else 'N/A'
                tipo_conta = conta_info.iloc[0]['tipo'] if not conta_info.empty and 'tipo' in conta_info.columns else 'Analitico'

                debitos = df_periodo[df_periodo['reduz_deb'] == conta_str]['valor'].sum()
                creditos = df_periodo[df_periodo['reduz_cred'] == conta_str]['valor'].sum()
                saldo = creditos - debitos

                balancete.append({
                    'Conta': conta_str,
                    'Descrição': nome_conta,
                    'Débitos': debitos,
                    'Créditos': creditos,
                    'Saldo': saldo,
                    'Tipo': tipo_conta
                })

            df_balancete = pd.DataFrame(balancete)

            if df_balancete.empty:
                st.warning("Nenhum lançamento encontrado no período.")
                return

            # SALVAR NO SESSION STATE
            st.session_state.balancete_preview = {
                'df_lancamentos': df_lancamentos,
                'df_plano_contas': df_plano_contas,
                'df_balancete': df_balancete,
                'data_inicio': data_inicio,
                'data_fim': data_fim
            }

        except ValueError:
            st.error("Formato de data inválido. Use DD/MM/AAAA.")
        except Exception as e:
            st.error(f"Erro ao gerar balancete: {e}")

    # VERIFICAR SE HÁ DADOS SALVOS NO SESSION STATE PARA EXIBIR
    if 'balancete_preview' in st.session_state:
        preview_data = st.session_state.balancete_preview
        df_lancamentos = preview_data['df_lancamentos']
        df_plano_contas = preview_data['df_plano_contas']
        df_balancete = preview_data['df_balancete']
        data_inicio = preview_data['data_inicio']
        data_fim = preview_data['data_fim']

        # VISUALIZAÇÃO PRÉVIA
        st.markdown("---")
        st.markdown("### 📊 Balancete de Verificação")
        st.caption(f"Período: {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

        # Métricas de totais
        total_debitos = df_balancete['Débitos'].sum()
        total_creditos = df_balancete['Créditos'].sum()
        total_saldo = df_balancete['Saldo'].sum()

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Débitos", f"R$ {total_debitos:,.2f}")
        with col2:
            st.metric("Total Créditos", f"R$ {total_creditos:,.2f}")
        with col3:
            st.metric("Saldo Total", f"R$ {total_saldo:,.2f}")

        # Tabela
        st.markdown("#### Detalhamento por Conta")
        df_display = df_balancete.copy()
        df_display['Débitos'] = df_display['Débitos'].apply(lambda x: f"R$ {x:,.2f}")
        df_display['Créditos'] = df_display['Créditos'].apply(lambda x: f"R$ {x:,.2f}")
        df_display['Saldo'] = df_display['Saldo'].apply(lambda x: f"R$ {x:,.2f}")

        # Aplicar negrito em contas sintéticas
        def highlight_sinteticas(row):
            if row.get('Tipo') == 'Sintetico':
                return ['font-weight: bold'] * len(row)
            return [''] * len(row)

        # Remover coluna Tipo da exibição
        df_display_sem_tipo = df_display.drop(columns=['Tipo'])
        styled_df = df_display_sem_tipo.style.apply(highlight_sinteticas, axis=1)

        st.dataframe(styled_df, use_container_width=True, hide_index=True)

        st.markdown("---")

        # Botão para gerar PDF
        if st.button("📄 Gerar PDF do Balancete", type="primary", use_container_width=True, key="btn_gerar_pdf_balancete"):
            with st.spinner("Gerando balancete em PDF..."):
                pdf_buffer = gerar_balancete_pdf(df_lancamentos, df_plano_contas,
                                                 empresa_info, logo_path,
                                                 data_inicio, data_fim)

            nome_arquivo = f"balancete_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"

            st.success("✅ Balancete gerado com sucesso!")
            st.download_button(
                label="⬇️ Baixar Balancete PDF",
                data=pdf_buffer,
                file_name=nome_arquivo,
                mime="application/pdf",
                key="download_balancete_pdf"
            )

def submenu_relatorio_livro_diario():
    st.subheader("6.3 Livro Diário")

    st.markdown("""
    Gera o Livro Diário com todos os lançamentos contábeis do período em ordem cronológica.
    """)

    empresa_info = carregar_empresa()
    if not empresa_info:
        st.warning("Cadastre os dados da empresa primeiro.")
        return

    logo_path = obter_logo_principal()

    col1, col2 = st.columns(2)
    today = datetime.date.today()
    first_day = today.replace(day=1)

    with col1:
        data_inicio_str = st.text_input("Data Inicial (DD/MM/AAAA)", value=first_day.strftime('%d/%m/%Y'))
    with col2:
        data_fim_str = st.text_input("Data Final (DD/MM/AAAA)", value=today.strftime('%d/%m/%Y'))

    if st.button("📖 Gerar Visualização do Livro Diário"):
        try:
            data_inicio = datetime.datetime.strptime(data_inicio_str, '%d/%m/%Y').date()
            data_fim = datetime.datetime.strptime(data_fim_str, '%d/%m/%Y').date()

            df_lancamentos = carregar_lancamentos_contabeis()

            if df_lancamentos.empty:
                st.warning("Nenhum lançamento contábil encontrado.")
                return

            # Converter data_lancamento para datetime se for string
            if df_lancamentos['data_lancamento'].dtype == 'object':
                df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'])

            # Filtrar lançamentos pelo período
            df_filtrado = df_lancamentos[
                (df_lancamentos['data_lancamento'].dt.date >= data_inicio) &
                (df_lancamentos['data_lancamento'].dt.date <= data_fim)
            ].copy()

            if df_filtrado.empty:
                st.warning(f"Nenhum lançamento encontrado no período de {data_inicio_str} a {data_fim_str}.")
                return

            # Ordenar por data
            df_filtrado = df_filtrado.sort_values('data_lancamento')

            # Carregar plano de contas para verificar tipo
            df_plano_contas = carregar_plano_contas()

            # SALVAR NO SESSION STATE
            st.session_state.livro_diario_preview = {
                'df_lancamentos': df_lancamentos,
                'df_filtrado': df_filtrado,
                'df_plano_contas': df_plano_contas,
                'data_inicio': data_inicio,
                'data_fim': data_fim,
                'data_inicio_str': data_inicio_str,
                'data_fim_str': data_fim_str
            }

        except ValueError:
            st.error("Formato de data inválido. Use DD/MM/AAAA.")
        except Exception as e:
            st.error(f"Erro ao gerar livro diário: {e}")

    # VERIFICAR SE HÁ DADOS SALVOS NO SESSION STATE PARA EXIBIR
    if 'livro_diario_preview' in st.session_state:
        preview_data = st.session_state.livro_diario_preview
        df_lancamentos = preview_data['df_lancamentos']
        df_filtrado = preview_data['df_filtrado']
        df_plano_contas = preview_data['df_plano_contas']
        data_inicio = preview_data['data_inicio']
        data_fim = preview_data['data_fim']
        data_inicio_str = preview_data['data_inicio_str']
        data_fim_str = preview_data['data_fim_str']

        # VISUALIZAÇÃO PRÉVIA
        st.markdown("---")
        st.markdown("### 📖 Visualização do Livro Diário")

        # Informações da empresa
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info(f"**Empresa:** {empresa_info['razao_social']}")
        with col2:
            st.info(f"**Período:** {data_inicio_str} a {data_fim_str}")
        with col3:
            st.info(f"**Total de Lançamentos:** {len(df_filtrado)}")

        # Métricas
        total_valores = df_filtrado['valor'].sum()

        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total de Lançamentos", len(df_filtrado))
        with col2:
            st.metric("Total Movimentado", f"R$ {total_valores:,.2f}")

        # Preparar dados para exibição
        df_display = df_filtrado.copy()
        df_display['Data'] = df_display['data_lancamento'].dt.strftime('%d/%m/%Y')
        df_display['Valor'] = df_display['valor'].apply(lambda x: f"R$ {x:,.2f}")
        df_display['Débito'] = df_display['reduz_deb'].fillna('-')
        df_display['Crédito'] = df_display['reduz_cred'].fillna('-')

        # Adicionar informação sobre tipo de conta
        def verificar_tipo_conta(codigo):
            if pd.isna(codigo) or codigo == '-':
                return 'Analitico'
            conta_info = df_plano_contas[df_plano_contas['codigo'] == str(codigo)]
            if not conta_info.empty and 'tipo' in conta_info.columns:
                return conta_info.iloc[0]['tipo']
            return 'Analitico'

        df_display['Tipo_Debito'] = df_display['reduz_deb'].apply(verificar_tipo_conta)
        df_display['Tipo_Credito'] = df_display['reduz_cred'].apply(verificar_tipo_conta)

        # Aplicar negrito em contas sintéticas
        def highlight_sinteticas_diario(row):
            styles = [''] * len(row)
            # Se débito é sintético, aplicar negrito na coluna Débito (índice 1)
            if row.get('Tipo_Debito') == 'Sintetico':
                styles[1] = 'font-weight: bold'
            # Se crédito é sintético, aplicar negrito na coluna Crédito (índice 2)
            if row.get('Tipo_Credito') == 'Sintetico':
                styles[2] = 'font-weight: bold'
            return styles

        # Mostrar tabela
        st.markdown("#### 📋 Lançamentos do Período")
        df_display_sem_tipo = df_display[['Data', 'Débito', 'Crédito', 'historico', 'Valor']]
        styled_df = df_display_sem_tipo.style.apply(highlight_sinteticas_diario, axis=1)

        st.dataframe(
            styled_df,
            use_container_width=True,
            hide_index=True
        )

        # Botão para gerar PDF
        st.markdown("---")
        if st.button("📄 Gerar PDF do Livro Diário", key="btn_gerar_pdf_livro_diario"):
            with st.spinner("Gerando livro diário em PDF..."):
                pdf_buffer = gerar_livro_diario_pdf(df_lancamentos, empresa_info,
                                                    logo_path, data_inicio, data_fim)

            nome_arquivo = f"livro_diario_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"

            st.success("✅ Livro Diário gerado com sucesso!")
            st.download_button(
                label="⬇️ Baixar Livro Diário PDF",
                data=pdf_buffer,
                file_name=nome_arquivo,
                mime="application/pdf",
                key="download_livro_diario_pdf"
            )

def submenu_relatorio_livro_razao():
    st.subheader("6.4 Livro Razão")

    st.markdown("""
    Gera o Livro Razão para uma conta contábil específica, mostrando todos os lançamentos e saldo acumulado.
    """)

    empresa_info = carregar_empresa()
    if not empresa_info:
        st.warning("Cadastre os dados da empresa primeiro.")
        return

    logo_path = obter_logo_principal()

    # Selecionar conta (apenas sintéticas)
    df_plano_contas = carregar_plano_contas()
    if df_plano_contas.empty:
        st.warning("Nenhuma conta contábil cadastrada.")
        return

    # Filtrar apenas contas sintéticas (que possuem o campo 'sintetica' == True ou similar)
    # Se não tiver este campo, mostrar todas
    if 'sintetica' in df_plano_contas.columns:
        df_sinteticas = df_plano_contas[df_plano_contas['sintetica'] == True]
    else:
        # Alternativa: considerar sintéticas as que têm filhas (códigos que começam com o seu código)
        df_sinteticas = df_plano_contas

    if df_sinteticas.empty:
        st.warning("Nenhuma conta sintética cadastrada.")
        return

    conta_selecionada = st.selectbox(
        "Selecione a Conta Contábil (Sintética):",
        options=df_sinteticas['codigo'].tolist(),
        format_func=lambda x: f"{x} - {df_sinteticas[df_sinteticas['codigo'] == x].iloc[0]['descricao']}"
    )

    col1, col2 = st.columns(2)
    today = datetime.date.today()
    first_day = today.replace(day=1)

    with col1:
        data_inicio_str = st.text_input("Data Inicial (DD/MM/AAAA)", value=first_day.strftime('%d/%m/%Y'))
    with col2:
        data_fim_str = st.text_input("Data Final (DD/MM/AAAA)", value=today.strftime('%d/%m/%Y'))

    if st.button("📘 Gerar Visualização do Livro Razão"):
        try:
            data_inicio = datetime.datetime.strptime(data_inicio_str, '%d/%m/%Y').date()
            data_fim = datetime.datetime.strptime(data_fim_str, '%d/%m/%Y').date()

            df_lancamentos = carregar_lancamentos_contabeis()

            if df_lancamentos.empty:
                st.warning("Nenhum lançamento contábil encontrado.")
                return

            # Converter data_lancamento para datetime se for string
            if df_lancamentos['data_lancamento'].dtype == 'object':
                df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'])

            # Normalizar códigos de conta
            df_lancamentos['reduz_deb'] = df_lancamentos['reduz_deb'].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
            )
            df_lancamentos['reduz_cred'] = df_lancamentos['reduz_cred'].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
            )

            # Filtrar lançamentos da conta no período
            df_filtrado = df_lancamentos[
                ((df_lancamentos['reduz_deb'] == conta_selecionada) |
                 (df_lancamentos['reduz_cred'] == conta_selecionada)) &
                (df_lancamentos['data_lancamento'].dt.date >= data_inicio) &
                (df_lancamentos['data_lancamento'].dt.date <= data_fim)
            ].copy()

            if df_filtrado.empty:
                st.warning(f"Nenhum lançamento encontrado para a conta {conta_selecionada} no período.")
                return

            # Ordenar por data
            df_filtrado = df_filtrado.sort_values('data_lancamento')

            # Obter descrição e tipo da conta
            conta_info = df_plano_contas[df_plano_contas['codigo'] == conta_selecionada].iloc[0]
            conta_descricao = conta_info['descricao']
            conta_tipo = conta_info['tipo'] if 'tipo' in conta_info.index else 'Analitico'

            # Calcular saldo acumulado
            saldo = 0.0
            saldos = []
            for _, row in df_filtrado.iterrows():
                if row['reduz_deb'] == conta_selecionada:
                    saldo += row['valor']
                if row['reduz_cred'] == conta_selecionada:
                    saldo -= row['valor']
                saldos.append(saldo)

            df_filtrado['Saldo Acumulado'] = saldos

            # SALVAR NO SESSION STATE
            st.session_state.livro_razao_preview = {
                'df_lancamentos': df_lancamentos,
                'df_plano_contas': df_plano_contas,
                'df_filtrado': df_filtrado,
                'conta_selecionada': conta_selecionada,
                'conta_descricao': conta_descricao,
                'conta_tipo': conta_tipo,
                'data_inicio': data_inicio,
                'data_fim': data_fim,
                'data_inicio_str': data_inicio_str,
                'data_fim_str': data_fim_str,
                'saldos': saldos
            }

        except ValueError:
            st.error("Formato de data inválido. Use DD/MM/AAAA.")
        except Exception as e:
            st.error(f"Erro ao gerar livro razão: {e}")

    # VERIFICAR SE HÁ DADOS SALVOS NO SESSION STATE PARA EXIBIR
    if 'livro_razao_preview' in st.session_state:
        preview_data = st.session_state.livro_razao_preview
        df_lancamentos = preview_data['df_lancamentos']
        df_plano_contas = preview_data['df_plano_contas']
        df_filtrado = preview_data['df_filtrado']
        conta_selecionada = preview_data['conta_selecionada']
        conta_descricao = preview_data['conta_descricao']
        conta_tipo = preview_data.get('conta_tipo', 'Analitico')
        data_inicio = preview_data['data_inicio']
        data_fim = preview_data['data_fim']
        data_inicio_str = preview_data['data_inicio_str']
        data_fim_str = preview_data['data_fim_str']
        saldos = preview_data['saldos']

        # VISUALIZAÇÃO PRÉVIA
        st.markdown("---")
        st.markdown("### 📘 Visualização do Livro Razão")

        # Informações
        col1, col2, col3 = st.columns(3)
        with col1:
            st.info(f"**Empresa:** {empresa_info['razao_social']}")
        with col2:
            # Aplicar negrito se conta for sintética
            if conta_tipo == 'Sintetico':
                st.info(f"**Conta:** **{conta_selecionada} - {conta_descricao}**")
            else:
                st.info(f"**Conta:** {conta_selecionada} - {conta_descricao}")
        with col3:
            st.info(f"**Período:** {data_inicio_str} a {data_fim_str}")

        # Métricas
        total_debitos = df_filtrado[df_filtrado['reduz_deb'] == conta_selecionada]['valor'].sum()
        total_creditos = df_filtrado[df_filtrado['reduz_cred'] == conta_selecionada]['valor'].sum()
        saldo_final = saldos[-1] if saldos else 0.0

        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total de Débitos", f"R$ {total_debitos:,.2f}")
        with col2:
            st.metric("Total de Créditos", f"R$ {total_creditos:,.2f}")
        with col3:
            st.metric("Saldo Final", f"R$ {saldo_final:,.2f}")

        # Preparar dados para exibição
        df_display = df_filtrado.copy()
        df_display['Data'] = df_display['data_lancamento'].dt.strftime('%d/%m/%Y')
        df_display['Débito'] = df_display.apply(
            lambda x: f"R$ {x['valor']:,.2f}" if x['reduz_deb'] == conta_selecionada else "-", axis=1
        )
        df_display['Crédito'] = df_display.apply(
            lambda x: f"R$ {x['valor']:,.2f}" if x['reduz_cred'] == conta_selecionada else "-", axis=1
        )
        df_display['Saldo'] = df_display['Saldo Acumulado'].apply(lambda x: f"R$ {x:,.2f}")

        # Mostrar tabela
        st.markdown("#### 📋 Movimentações da Conta")
        st.dataframe(
            df_display[['Data', 'historico', 'Débito', 'Crédito', 'Saldo']],
            use_container_width=True,
            hide_index=True
        )

        # Botão para gerar PDF
        st.markdown("---")
        if st.button("📄 Gerar PDF do Livro Razão", key="btn_gerar_pdf_livro_razao"):
            with st.spinner("Gerando livro razão em PDF..."):
                pdf_buffer = gerar_livro_razao_pdf(df_lancamentos, df_plano_contas,
                                                   empresa_info, logo_path, conta_selecionada,
                                                   data_inicio, data_fim)

            nome_arquivo = f"livro_razao_conta_{conta_selecionada}_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.pdf"

            st.success("✅ Livro Razão gerado com sucesso!")
            st.download_button(
                label="⬇️ Baixar Livro Razão PDF",
                data=pdf_buffer,
                file_name=nome_arquivo,
                mime="application/pdf",
                key="download_livro_razao_pdf"
            )

def submenu_relatorio_balanco_patrimonial():
    st.subheader("6.5 Balanço Patrimonial")

    st.markdown("""
    Gera o Balanço Patrimonial mostrando Ativo, Passivo e Patrimônio Líquido em uma data específica.
    """)

    empresa_info = carregar_empresa()
    if not empresa_info:
        st.warning("Cadastre os dados da empresa primeiro.")
        return

    logo_path = obter_logo_principal()

    data_referencia_str = st.text_input(
        "Data de Referência (DD/MM/AAAA)",
        value=datetime.date.today().strftime('%d/%m/%Y')
    )

    if st.button("💼 Gerar Visualização do Balanço Patrimonial"):
        try:
            data_referencia = datetime.datetime.strptime(data_referencia_str, '%d/%m/%Y').date()

            df_lancamentos = carregar_lancamentos_contabeis()
            df_plano_contas = carregar_plano_contas()

            if df_lancamentos.empty:
                st.warning("Nenhum lançamento contábil encontrado.")
                return

            if df_plano_contas.empty:
                st.warning("Nenhum plano de contas cadastrado.")
                return

            # Converter data_lancamento para datetime se for string
            if df_lancamentos['data_lancamento'].dtype == 'object':
                df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'])

            # Filtrar lançamentos até a data de referência
            df_filtrado = df_lancamentos[df_lancamentos['data_lancamento'].dt.date <= data_referencia].copy()

            if df_filtrado.empty:
                st.warning(f"Nenhum lançamento encontrado até {data_referencia_str}.")
                return

            # Normalizar códigos de conta
            df_filtrado['reduz_deb'] = df_filtrado['reduz_deb'].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
            )
            df_filtrado['reduz_cred'] = df_filtrado['reduz_cred'].apply(
                lambda x: str(int(float(x))) if pd.notna(x) and str(x).replace('.', '').replace('-', '').isdigit() else str(x) if pd.notna(x) else None
            )

            # Calcular saldos por conta
            saldos_contas = {}
            for _, lanc in df_filtrado.iterrows():
                conta_deb = lanc['reduz_deb']
                conta_cred = lanc['reduz_cred']
                valor = lanc['valor']

                if pd.notna(conta_deb) and conta_deb not in saldos_contas:
                    saldos_contas[conta_deb] = 0.0
                if pd.notna(conta_cred) and conta_cred not in saldos_contas:
                    saldos_contas[conta_cred] = 0.0

                if pd.notna(conta_deb):
                    saldos_contas[conta_deb] += valor
                if pd.notna(conta_cred):
                    saldos_contas[conta_cred] -= valor

            # Classificar contas por tipo
            ativo = {}
            passivo = {}
            patrimonio_liquido = {}

            # Debug: verificar saldos_contas
            if not saldos_contas:
                st.warning("⚠️ Nenhum saldo de conta foi calculado. Verifique se há lançamentos contábeis.")

            for codigo, saldo in saldos_contas.items():
                if pd.isna(codigo) or codigo is None:
                    continue

                # Apenas contas com saldo diferente de zero
                if abs(saldo) < 0.01:
                    continue

                conta_info = df_plano_contas[df_plano_contas['codigo'] == str(codigo)]
                if conta_info.empty:
                    st.warning(f"⚠️ Conta {codigo} não encontrada no plano de contas (Saldo: R$ {saldo:,.2f})")
                    continue

                descricao = conta_info.iloc[0]['descricao']
                tipo_conta = conta_info.iloc[0]['tipo'] if 'tipo' in conta_info.columns else 'Analitico'
                classificacao = conta_info.iloc[0]['classificacao'] if 'classificacao' in conta_info.columns else ''

                # Classificar por código da conta (padrão contábil brasileiro)
                # 1.x.x.x = Ativo
                # 2.x.x.x = Passivo
                # 3.x.x.x = Patrimônio Líquido / Resultado
                codigo_str = str(codigo)
                primeiro_digito = codigo_str[0] if len(codigo_str) > 0 else ''

                if primeiro_digito == '1':
                    ativo[codigo] = {'descricao': descricao, 'saldo': abs(saldo), 'tipo': tipo_conta, 'classificacao': classificacao}
                elif primeiro_digito == '2':
                    passivo[codigo] = {'descricao': descricao, 'saldo': abs(saldo), 'tipo': tipo_conta, 'classificacao': classificacao}
                elif primeiro_digito == '3':
                    patrimonio_liquido[codigo] = {'descricao': descricao, 'saldo': abs(saldo), 'tipo': tipo_conta, 'classificacao': classificacao}
                else:
                    # Contas 4, 5, 6, 7 (Receitas/Despesas) não entram no Balanço Patrimonial
                    pass

            # Calcular totais
            total_ativo = sum(c['saldo'] for c in ativo.values())
            total_passivo = sum(c['saldo'] for c in passivo.values())
            total_patrimonio = sum(c['saldo'] for c in patrimonio_liquido.values())
            total_passivo_pl = total_passivo + total_patrimonio

            # SALVAR NO SESSION STATE
            st.session_state.balanco_patrimonial_preview = {
                'df_lancamentos': df_lancamentos,
                'df_plano_contas': df_plano_contas,
                'ativo': ativo,
                'passivo': passivo,
                'patrimonio_liquido': patrimonio_liquido,
                'total_ativo': total_ativo,
                'total_passivo': total_passivo,
                'total_patrimonio': total_patrimonio,
                'total_passivo_pl': total_passivo_pl,
                'data_referencia': data_referencia,
                'data_referencia_str': data_referencia_str
            }

        except ValueError:
            st.error("Formato de data inválido. Use DD/MM/AAAA.")
        except Exception as e:
            st.error(f"Erro ao gerar balanço patrimonial: {e}")

    # VERIFICAR SE HÁ DADOS SALVOS NO SESSION STATE PARA EXIBIR
    if 'balanco_patrimonial_preview' in st.session_state:
        preview_data = st.session_state.balanco_patrimonial_preview
        df_lancamentos = preview_data['df_lancamentos']
        df_plano_contas = preview_data['df_plano_contas']
        ativo = preview_data['ativo']
        passivo = preview_data['passivo']
        patrimonio_liquido = preview_data['patrimonio_liquido']
        total_ativo = preview_data['total_ativo']
        total_passivo = preview_data['total_passivo']
        total_patrimonio = preview_data['total_patrimonio']
        total_passivo_pl = preview_data['total_passivo_pl']
        data_referencia = preview_data['data_referencia']
        data_referencia_str = preview_data['data_referencia_str']

        # VISUALIZAÇÃO PRÉVIA
        st.markdown("---")
        st.markdown("### 💼 Visualização do Balanço Patrimonial")

        # Informações
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"**Empresa:** {empresa_info['razao_social']}")
        with col2:
            st.info(f"**Data de Referência:** {data_referencia_str}")

        # Métricas principais
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total do Ativo", f"R$ {total_ativo:,.2f}")
        with col2:
            st.metric("Total do Passivo", f"R$ {total_passivo:,.2f}")
        with col3:
            st.metric("Patrimônio Líquido", f"R$ {total_patrimonio:,.2f}")

        # Criar tabelas lado a lado
        col_esq, col_dir = st.columns(2)

        with col_esq:
            st.markdown("#### 📊 ATIVO")
            if ativo:
                df_ativo = pd.DataFrame([
                    {
                        'Conta': codigo,
                        'Classificação': info.get('classificacao', ''),
                        'Descrição': info['descricao'],
                        'Saldo': f"R$ {info['saldo']:,.2f}",
                        'Tipo': info.get('tipo', 'Analitico')
                    }
                    for codigo, info in sorted(ativo.items())
                ])

                # Aplicar negrito em contas sintéticas
                def highlight_sinteticas_ativo(row):
                    if df_ativo.loc[row.name, 'Tipo'] == 'Sintetico':
                        return ['font-weight: bold'] * len(row)
                    return [''] * len(row)

                df_ativo_sem_tipo = df_ativo.drop(columns=['Tipo'])
                styled_df_ativo = df_ativo_sem_tipo.style.apply(highlight_sinteticas_ativo, axis=1)

                st.dataframe(styled_df_ativo, use_container_width=True, hide_index=True)
                st.markdown(f"**Total do Ativo: R$ {total_ativo:,.2f}**")
            else:
                st.warning("Nenhuma conta de Ativo encontrada")

        with col_dir:
            st.markdown("#### 📊 PASSIVO")
            if passivo:
                df_passivo = pd.DataFrame([
                    {
                        'Conta': codigo,
                        'Classificação': info.get('classificacao', ''),
                        'Descrição': info['descricao'],
                        'Saldo': f"R$ {info['saldo']:,.2f}",
                        'Tipo': info.get('tipo', 'Analitico')
                    }
                    for codigo, info in sorted(passivo.items())
                ])

                # Aplicar negrito em contas sintéticas
                def highlight_sinteticas_passivo(row):
                    if df_passivo.loc[row.name, 'Tipo'] == 'Sintetico':
                        return ['font-weight: bold'] * len(row)
                    return [''] * len(row)

                df_passivo_sem_tipo = df_passivo.drop(columns=['Tipo'])
                styled_df_passivo = df_passivo_sem_tipo.style.apply(highlight_sinteticas_passivo, axis=1)

                st.dataframe(styled_df_passivo, use_container_width=True, hide_index=True)
                st.markdown(f"**Total do Passivo: R$ {total_passivo:,.2f}**")
            else:
                st.warning("Nenhuma conta de Passivo encontrada")

            st.markdown("#### 📊 PATRIMÔNIO LÍQUIDO")
            if patrimonio_liquido:
                df_pl = pd.DataFrame([
                    {
                        'Conta': codigo,
                        'Classificação': info.get('classificacao', ''),
                        'Descrição': info['descricao'],
                        'Saldo': f"R$ {info['saldo']:,.2f}",
                        'Tipo': info.get('tipo', 'Analitico')
                    }
                    for codigo, info in sorted(patrimonio_liquido.items())
                ])

                # Aplicar negrito em contas sintéticas
                def highlight_sinteticas_pl(row):
                    if df_pl.loc[row.name, 'Tipo'] == 'Sintetico':
                        return ['font-weight: bold'] * len(row)
                    return [''] * len(row)

                df_pl_sem_tipo = df_pl.drop(columns=['Tipo'])
                styled_df_pl = df_pl_sem_tipo.style.apply(highlight_sinteticas_pl, axis=1)

                st.dataframe(styled_df_pl, use_container_width=True, hide_index=True)
                st.markdown(f"**Total do PL: R$ {total_patrimonio:,.2f}**")
            else:
                st.warning("Nenhuma conta de Patrimônio Líquido encontrada")

            st.markdown(f"**Total Passivo + PL: R$ {total_passivo_pl:,.2f}**")

        # Verificação de balanceamento
        diferenca = abs(total_ativo - total_passivo_pl)
        if diferenca < 0.01:
            st.success("✅ Balanço balanceado! Ativo = Passivo + PL")
        else:
            st.warning(f"⚠️ Diferença encontrada: R$ {diferenca:,.2f}")

        # Botão para gerar PDF
        st.markdown("---")
        if st.button("📄 Gerar PDF do Balanço Patrimonial", key="btn_gerar_pdf_balanco"):
            with st.spinner("Gerando balanço patrimonial em PDF..."):
                pdf_buffer = gerar_balanco_patrimonial_pdf(df_lancamentos, df_plano_contas,
                                                           empresa_info, logo_path,
                                                           data_referencia)

            nome_arquivo = f"balanco_patrimonial_{data_referencia.strftime('%Y%m%d')}.pdf"

            st.success("✅ Balanço Patrimonial gerado com sucesso!")
            st.download_button(
                label="⬇️ Baixar Balanço Patrimonial PDF",
                data=pdf_buffer,
                file_name=nome_arquivo,
                mime="application/pdf",
                key="download_balanco_patrimonial_pdf"
            )

def submenu_conciliacao_banco_contabil():
    st.subheader("5.1 Conciliação Banco x Contábil")

    st.markdown("""
    Esta ferramenta compara o saldo final do extrato bancário com o saldo contábil da conta,
    verificando se há diferenças entre os valores registrados no banco e na contabilidade.
    """)

    # Carregar contas bancárias
    df_contas = carregar_cadastro_contas()
    if df_contas.empty:
        st.warning("O Cadastro de Contas (Menu 1.1) está vazio. É necessário cadastrar as contas primeiro.")
        return

    # Limpar cache e carregar dados dos bancos para obter o nome atualizado
    ler_bancos_associados.clear()
    df_bancos = ler_bancos_associados()

    # Fazer merge para adicionar nome do banco
    if not df_bancos.empty:
        # Normalizar códigos de banco - remover zeros à esquerda e converter para int depois string
        def normalizar_codigo_banco(codigo):
            try:
                return str(int(str(codigo).strip()))
            except:
                return str(codigo).strip()

        df_bancos['codigo_banco_normalizado'] = df_bancos['codigo_banco'].apply(normalizar_codigo_banco)
        df_contas['Codigo_Banco_Normalizado'] = df_contas['Codigo_Banco'].apply(normalizar_codigo_banco)

        df_contas = pd.merge(
            df_contas,
            df_bancos[['codigo_banco_normalizado', 'nome_banco']],
            left_on='Codigo_Banco_Normalizado',
            right_on='codigo_banco_normalizado',
            how='left'
        )
        df_contas.rename(columns={'nome_banco': 'Nome Banco'}, inplace=True)
        df_contas.drop(columns=['codigo_banco_normalizado', 'Codigo_Banco_Normalizado'], inplace=True, errors='ignore')
    else:
        df_contas['Nome Banco'] = 'N/A'

    # Filtrar contas que têm conta contábil vinculada
    df_contas_vinculadas = df_contas[df_contas['Conta Contábil'].notna()].copy()

    if df_contas_vinculadas.empty:
        st.warning("Nenhuma conta bancária possui conta contábil vinculada. Configure no Menu 1.1.")
        return

    # Garantir que Nome Banco existe
    if 'Nome Banco' not in df_contas_vinculadas.columns:
        df_contas_vinculadas['Nome Banco'] = 'N/A'

    df_contas_vinculadas['Display'] = (
        df_contas_vinculadas['Codigo_Banco'].astype(str) + " - " +
        df_contas_vinculadas['Agencia'].astype(str) + "/" +
        df_contas_vinculadas['Conta'].astype(str)
    )

    # Opção de conciliação
    tipo_conciliacao = st.radio(
        "Tipo de Conciliação:",
        ["Individual", "Todos os Bancos"],
        horizontal=True,
        key="tipo_conciliacao"
    )

    # Layout dos filtros
    if tipo_conciliacao == "Individual":
        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            conta_selecionada_display = st.selectbox(
                "Selecione a Conta Bancária:",
                options=df_contas_vinculadas['Display'].tolist(),
                key="conc_banco_conta"
            )
    else:
        col2, col3 = st.columns([1, 1])

    today = datetime.date.today()
    first_day_of_month = today.replace(day=1)

    with col2:
        data_inicio_str = st.text_input(
            "Data Inicial (DD/MM/AAAA)",
            value=first_day_of_month.strftime('%d/%m/%Y'),
            key="conc_banco_data_inicio"
        )

    with col3:
        data_fim_str = st.text_input(
            "Data Final (DD/MM/AAAA)",
            value=today.strftime('%d/%m/%Y'),
            key="conc_banco_data_fim"
        )

    if st.button("🔍 Realizar Conciliação"):
        try:
            data_inicio = datetime.datetime.strptime(data_inicio_str, '%d/%m/%Y').date()
            data_fim = datetime.datetime.strptime(data_fim_str, '%d/%m/%Y').date()

            # Definir lista de contas a processar
            if tipo_conciliacao == "Individual":
                contas_processar = [df_contas_vinculadas[
                    df_contas_vinculadas['Display'] == conta_selecionada_display
                ].iloc[0]]
            else:
                contas_processar = df_contas_vinculadas.to_dict('records')

            # Lista para armazenar resultados
            resultados_conciliacao = []

            # Processar cada conta
            for conta_selecionada in contas_processar:
                conta_ofx_normalizada = conta_selecionada['Conta_OFX_Normalizada']
                conta_contabil = conta_selecionada['Conta Contábil']
                conta_contabil_negativo = conta_selecionada.get('Conta Contábil (-)')
                saldo_inicial_banco = conta_selecionada.get('Saldo Inicial', 0.0)

                # Display da conta
                conta_display = (
                    str(conta_selecionada.get('Codigo_Banco', '')) + " - " +
                    str(conta_selecionada.get('Agencia', '')) + "/" +
                    str(conta_selecionada.get('Conta', ''))
                )

                # === SALDO BANCÁRIO ===
                # Buscar extrato bancário do período
                df_extrato_banco = carregar_extrato_bancario_historico(
                    conta_ofx_normalizada, data_inicio, data_fim
                )

                # Calcular saldo bancário
                if not df_extrato_banco.empty:
                    movimentacoes_banco = df_extrato_banco['Valor'].sum()
                else:
                    movimentacoes_banco = 0.0

                # Buscar movimentações anteriores para calcular saldo inicial real
                data_cadastro = conta_selecionada.get('Data Inicial Saldo')
                if data_cadastro and pd.notna(data_cadastro):
                    try:
                        data_inicial_cadastro = pd.to_datetime(data_cadastro, format='%d/%m/%Y').date()
                    except:
                        data_inicial_cadastro = datetime.date(2000, 1, 1)
                else:
                    data_inicial_cadastro = datetime.date(2000, 1, 1)

                data_anterior = data_inicio - datetime.timedelta(days=1)
                df_anterior_banco = carregar_extrato_bancario_historico(
                    conta_ofx_normalizada, data_inicial_cadastro, data_anterior
                )

                saldo_anterior_banco = saldo_inicial_banco + (
                    df_anterior_banco['Valor'].sum() if not df_anterior_banco.empty else 0.0
                )
                saldo_final_banco = saldo_anterior_banco + movimentacoes_banco

                # === SALDO CONTÁBIL ===
                # Buscar lançamentos contábeis da conta
                df_lancamentos = carregar_lancamentos_contabeis()

                if not df_lancamentos.empty:
                    # Converter conta contábil para string e normalizar
                    try:
                        # Limpar e converter a conta contábil
                        conta_contabil_limpa = str(conta_contabil).strip()
                        # Tentar converter para inteiro se for número
                        try:
                            conta_contabil_str = str(int(float(conta_contabil_limpa)))
                        except:
                            conta_contabil_str = conta_contabil_limpa
                    except:
                        st.error(f"Erro ao processar conta contábil: {conta_contabil}")
                        continue

                    # Converter data para datetime
                    df_lancamentos['data_lancamento_dt'] = pd.to_datetime(
                        df_lancamentos['data_lancamento'], errors='coerce'
                    ).dt.date

                    # Converter colunas de débito e crédito para string, tratando valores nulos
                    df_lancamentos['reduz_deb_str'] = df_lancamentos['reduz_deb'].fillna('').astype(str).str.strip()
                    df_lancamentos['reduz_cred_str'] = df_lancamentos['reduz_cred'].fillna('').astype(str).str.strip()

                    # Converter valores numéricos para inteiro quando possível
                    def normalizar_conta(valor):
                        if valor == '' or valor == 'nan':
                            return ''
                        try:
                            return str(int(float(valor)))
                        except:
                            return valor

                    df_lancamentos['reduz_deb_str'] = df_lancamentos['reduz_deb_str'].apply(normalizar_conta)
                    df_lancamentos['reduz_cred_str'] = df_lancamentos['reduz_cred_str'].apply(normalizar_conta)

                    # Lançamentos até o dia anterior (para saldo inicial)
                    df_anterior_contabil = df_lancamentos[
                        df_lancamentos['data_lancamento_dt'] < data_inicio
                    ].copy()

                    # Lançamentos do período
                    df_periodo_contabil = df_lancamentos[
                        (df_lancamentos['data_lancamento_dt'] >= data_inicio) &
                        (df_lancamentos['data_lancamento_dt'] <= data_fim)
                    ].copy()

                    # Calcular saldo anterior contábil
                    debitos_anteriores = df_anterior_contabil[
                        df_anterior_contabil['reduz_deb_str'] == conta_contabil_str
                    ]['valor'].sum()

                    creditos_anteriores = df_anterior_contabil[
                        df_anterior_contabil['reduz_cred_str'] == conta_contabil_str
                    ]['valor'].sum()

                    saldo_anterior_contabil = creditos_anteriores - debitos_anteriores

                    # Calcular movimentações do período
                    debitos_periodo = df_periodo_contabil[
                        df_periodo_contabil['reduz_deb_str'] == conta_contabil_str
                    ]['valor'].sum()

                    creditos_periodo = df_periodo_contabil[
                        df_periodo_contabil['reduz_cred_str'] == conta_contabil_str
                    ]['valor'].sum()

                    movimentacoes_contabil = creditos_periodo - debitos_periodo
                    saldo_final_contabil = saldo_anterior_contabil + movimentacoes_contabil

                    # === AJUSTE PARA SALDO NEGATIVO ===
                    # Se o saldo bancário é negativo e existe conta contábil negativa configurada,
                    # precisa somar o saldo da conta contábil negativa (passivo)
                    if saldo_final_banco < 0 and conta_contabil_negativo and pd.notna(conta_contabil_negativo):
                        try:
                            # Normalizar conta contábil negativa
                            conta_contabil_negativo_limpa = str(conta_contabil_negativo).strip()
                            try:
                                conta_contabil_negativo_str = str(int(float(conta_contabil_negativo_limpa)))
                            except:
                                conta_contabil_negativo_str = conta_contabil_negativo_limpa

                            # Calcular saldo anterior da conta negativa
                            debitos_anteriores_neg = df_anterior_contabil[
                                df_anterior_contabil['reduz_deb_str'] == conta_contabil_negativo_str
                            ]['valor'].sum()

                            creditos_anteriores_neg = df_anterior_contabil[
                                df_anterior_contabil['reduz_cred_str'] == conta_contabil_negativo_str
                            ]['valor'].sum()

                            saldo_anterior_contabil_neg = creditos_anteriores_neg - debitos_anteriores_neg

                            # Calcular movimentações do período da conta negativa
                            debitos_periodo_neg = df_periodo_contabil[
                                df_periodo_contabil['reduz_deb_str'] == conta_contabil_negativo_str
                            ]['valor'].sum()

                            creditos_periodo_neg = df_periodo_contabil[
                                df_periodo_contabil['reduz_cred_str'] == conta_contabil_negativo_str
                            ]['valor'].sum()

                            movimentacoes_contabil_neg = creditos_periodo_neg - debitos_periodo_neg
                            saldo_final_contabil_neg = saldo_anterior_contabil_neg + movimentacoes_contabil_neg

                            # O saldo contábil total é: saldo da conta principal + saldo da conta negativa
                            # Como a conta negativa é passivo (crédito), ela representa o valor negativo
                            # Então: Saldo Real = Saldo Ativo - Saldo Passivo
                            saldo_final_contabil = saldo_final_contabil - saldo_final_contabil_neg

                        except Exception as e:
                            # Se houver erro, mantém o saldo original
                            pass

                    # Debug: mostrar quantos lançamentos foram encontrados (apenas no modo individual)
                    if tipo_conciliacao == "Individual":
                        total_lanc_deb = len(df_periodo_contabil[df_periodo_contabil['reduz_deb_str'] == conta_contabil_str])
                        total_lanc_cred = len(df_periodo_contabil[df_periodo_contabil['reduz_cred_str'] == conta_contabil_str])

                        with st.expander("🔍 Debug - Lançamentos Encontrados"):
                            st.write(f"Conta Contábil procurada: **{conta_contabil_str}**")
                            st.write(f"Total de lançamentos a débito: **{total_lanc_deb}**")
                            st.write(f"Total de lançamentos a crédito: **{total_lanc_cred}**")

                            if total_lanc_deb > 0 or total_lanc_cred > 0:
                                st.write("Lançamentos do período:")
                                lanc_conta = df_periodo_contabil[
                                    (df_periodo_contabil['reduz_deb_str'] == conta_contabil_str) |
                                    (df_periodo_contabil['reduz_cred_str'] == conta_contabil_str)
                                ][['data_lancamento', 'reduz_deb_str', 'reduz_cred_str', 'valor', 'historico']]
                                st.dataframe(lanc_conta)
                else:
                    saldo_anterior_contabil = 0.0
                    debitos_periodo = 0.0
                    creditos_periodo = 0.0
                    movimentacoes_contabil = 0.0
                    saldo_final_contabil = 0.0

                # Calcular diferença
                diferenca = saldo_final_banco - saldo_final_contabil
                status_conciliacao = 'Conciliado' if abs(diferenca) < 0.01 else 'Não Conciliado'

                # Buscar nome do banco
                codigo_banco = str(conta_selecionada.get('Codigo_Banco', '')).strip()
                nome_banco = conta_selecionada.get('Nome Banco', '')

                # Se não encontrou no dict, buscar no df_bancos
                if not nome_banco or nome_banco == 'N/A' or pd.isna(nome_banco):
                    if not df_bancos.empty and codigo_banco:
                        # Normalizar código para comparação (remover zeros à esquerda)
                        try:
                            codigo_normalizado = str(int(codigo_banco))
                        except:
                            codigo_normalizado = codigo_banco

                        # Criar coluna normalizada temporária se não existir
                        if 'codigo_banco_normalizado' not in df_bancos.columns:
                            df_bancos['codigo_banco_normalizado'] = df_bancos['codigo_banco'].apply(
                                lambda x: str(int(str(x).strip())) if str(x).strip().isdigit() else str(x).strip()
                            )

                        banco_info = df_bancos[df_bancos['codigo_banco_normalizado'] == codigo_normalizado]
                        if not banco_info.empty:
                            nome_banco = banco_info.iloc[0]['nome_banco']
                        else:
                            nome_banco = 'Banco não identificado'
                    else:
                        nome_banco = 'N/A'

                # Armazenar resultado
                resultados_conciliacao.append({
                    'Banco': codigo_banco,
                    'Nome Banco': nome_banco,
                    'Conta': conta_display,
                    'Conta Contábil': conta_contabil,
                    'Saldo Banco': saldo_final_banco,
                    'Saldo Contábil': saldo_final_contabil,
                    'Diferença': diferenca,
                    'Status': status_conciliacao
                })

                # === EXIBIR RESULTADOS (apenas no modo individual) ===
                if tipo_conciliacao == "Individual":
                    st.markdown("---")
                    st.subheader("Resultado da Conciliação")

                    # Informações da conta
                    st.markdown(f"**Conta Bancária:** {conta_display}")
                    st.markdown(f"**Conta Contábil:** {conta_contabil}")
                    st.markdown(f"**Período:** {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")

                    st.markdown("---")

                    # Tabela comparativa
                    col_banco, col_contabil = st.columns(2)

                    with col_banco:
                        st.markdown("### 💰 Saldo Bancário")
                        st.metric("Saldo Inicial", formatar_moeda(saldo_anterior_banco))
                        st.metric("Movimentações", formatar_moeda(movimentacoes_banco))
                        st.metric("Saldo Final", formatar_moeda(saldo_final_banco))

                    with col_contabil:
                        st.markdown("### 📊 Saldo Contábil")
                        st.metric("Saldo Inicial", formatar_moeda(saldo_anterior_contabil))
                        st.metric("Créditos - Débitos", formatar_moeda(movimentacoes_contabil))
                        st.metric("Saldo Final", formatar_moeda(saldo_final_contabil))

                    # Verificar diferença
                    st.markdown("---")

                    if abs(diferenca) < 0.01:  # Tolerância de 1 centavo
                        st.success("✅ **SALDOS CONCILIADOS** - Os saldos bancário e contábil estão corretos!")
                        st.balloons()
                    else:
                        st.error(f"❌ **DIFERENÇA ENCONTRADA** - Há uma diferença de {formatar_moeda(diferenca)}")
                        st.markdown(f"**Diferença:** {formatar_moeda(abs(diferenca))}")

                        if diferenca > 0:
                            st.info("O saldo bancário está **maior** que o saldo contábil.")
                        else:
                            st.info("O saldo bancário está **menor** que o saldo contábil.")

                    # Tabela resumo
                    st.markdown("---")
                    st.subheader("Resumo Detalhado")

                    dados_resumo = {
                        'Descrição': [
                            'Saldo Inicial Banco',
                            'Saldo Inicial Contábil',
                            'Movimentações Banco',
                            'Débitos Contábil',
                            'Créditos Contábil',
                            'Saldo Final Banco',
                            'Saldo Final Contábil',
                            'Diferença'
                        ],
                        'Valor (R$)': [
                            saldo_anterior_banco,
                            saldo_anterior_contabil,
                            movimentacoes_banco,
                            debitos_periodo,
                            creditos_periodo,
                            saldo_final_banco,
                            saldo_final_contabil,
                            diferenca
                        ],
                        'Status': [
                            '',
                            '',
                            '',
                            '',
                            '',
                            '',
                            '',
                            'Conciliado' if abs(diferenca) < 0.01 else 'Não Conciliado'
                        ]
                    }

                    df_resumo = pd.DataFrame(dados_resumo)
                    st.dataframe(df_resumo, hide_index=True, use_container_width=True)

            # === EXIBIR RESULTADOS CONSOLIDADOS (modo todos os bancos) ===
            if tipo_conciliacao == "Todos os Bancos" and resultados_conciliacao:
                st.markdown("---")
                st.subheader("📊 Resultado da Conciliação - Todos os Bancos")
                st.markdown(f"**Período:** {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")
                st.markdown("---")

                # Criar DataFrame com os resultados
                df_resultados = pd.DataFrame(resultados_conciliacao)

                # Formatar valores monetários
                df_resultados['Saldo Banco R$'] = df_resultados['Saldo Banco'].apply(formatar_moeda)
                df_resultados['Saldo Contábil R$'] = df_resultados['Saldo Contábil'].apply(formatar_moeda)
                df_resultados['Diferença R$'] = df_resultados['Diferença'].apply(formatar_moeda)

                # Selecionar colunas para exibição
                df_display = df_resultados[['Banco', 'Nome Banco', 'Conta', 'Saldo Banco R$',
                                            'Saldo Contábil R$', 'Diferença R$', 'Status']]

                # Estilizar a tabela com cores
                def highlight_status(row):
                    if row['Status'] == 'Conciliado':
                        return ['background-color: #d4edda'] * len(row)
                    else:
                        return ['background-color: #f8d7da'] * len(row)

                st.dataframe(
                    df_display.style.apply(highlight_status, axis=1),
                    hide_index=True,
                    use_container_width=True
                )

                # Resumo geral
                total_contas = len(df_resultados)
                contas_conciliadas = len(df_resultados[df_resultados['Status'] == 'Conciliado'])
                contas_nao_conciliadas = total_contas - contas_conciliadas

                st.markdown("---")
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.metric("Total de Contas", total_contas)
                with col2:
                    st.metric("✅ Conciliadas", contas_conciliadas)
                with col3:
                    st.metric("❌ Não Conciliadas", contas_nao_conciliadas)

                if contas_nao_conciliadas == 0:
                    st.success("🎉 **TODAS AS CONTAS ESTÃO CONCILIADAS!**")
                    st.balloons()
                else:
                    st.warning(f"⚠️ **ATENÇÃO**: {contas_nao_conciliadas} conta(s) com diferença(s)")

        except ValueError:
            st.error("Formato de data inválido. Use DD/MM/AAAA.")
        except Exception as e:
            st.error(f"Erro ao realizar conciliação: {e}")

def submenu_conciliacao_contas_negativas():
    st.subheader("4.2 Conciliação Contas Negativas")
    
    st.markdown("""
    Esta ferramenta analisa o saldo diário de uma conta bancária e gera lançamentos contábeis automáticos 
    para cobrir os saldos negativos, transferindo-os para uma conta de passivo (empréstimo), e estornando 
    o lançamento quando o saldo volta a ficar positivo.
    """)

    # --- Seleção de Conta e Período ---
    df_contas = carregar_cadastro_contas()
    if df_contas.empty:
        st.warning("O Cadastro de Contas (Menu 1.1) está vazio. É necessário cadastrar as contas primeiro.")
        return

    df_contas['Display'] = df_contas['Agencia'].astype(str) + " / " + df_contas['Conta'].astype(str)
    contas_display = df_contas[['Display']].drop_duplicates().sort_values('Display')

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        conta_selecionada_display = st.selectbox("Selecione a Conta Bancária para análise:", options=contas_display['Display'].tolist(), key="ccn_conta_select")
    
    today = datetime.date.today()
    first_day_of_month = today.replace(day=1)
    last_month = first_day_of_month - datetime.timedelta(days=1)
    
    with col2:
        data_inicio_str = st.text_input("Data de Início da Análise (DD/MM/AAAA)", value=last_month.replace(day=1).strftime('%d/%m/%Y'), key="ccn_data_inicio")
    with col3:
        data_fim_str = st.text_input("Data Final da Análise (DD/MM/AAAA)", value=last_month.strftime('%d/%m/%Y'), key="ccn_data_fim")

    # Inicializa o estado para os lançamentos propostos
    if 'lancamentos_negativos_propostos' not in st.session_state:
        st.session_state.lancamentos_negativos_propostos = pd.DataFrame()

    # --- Botão de Análise ---
    if st.button("🔍 Analisar Saldo Negativo"):
        try:
            data_inicio = datetime.datetime.strptime(data_inicio_str, '%d/%m/%Y').date()
            data_fim = datetime.datetime.strptime(data_fim_str, '%d/%m/%Y').date()
            
            conta_selecionada_row = df_contas[df_contas['Display'] == conta_selecionada_display].iloc[0]
            
            with st.spinner("Analisando saldos e gerando lançamentos..."):
                lancamentos_propostos_df = gerar_lancamentos_saldo_negativo(conta_selecionada_row, data_inicio, data_fim)
                st.session_state.lancamentos_negativos_propostos = lancamentos_propostos_df
        
        except ValueError:
            st.error("Formato de data inválido. Por favor, use DD/MM/AAAA.")
            st.session_state.lancamentos_negativos_propostos = pd.DataFrame()
        except Exception as e:
            st.error(f"Ocorreu um erro inesperado durante a análise: {e}")
            st.session_state.lancamentos_negativos_propostos = pd.DataFrame()

    # --- Exibição dos Lançamentos Propostos e Botão de Salvar ---
    if not st.session_state.lancamentos_negativos_propostos.empty:
        st.markdown("---")
        st.subheader("Lançamentos de Ajuste Propostos")
        
        df_proposto = st.session_state.lancamentos_negativos_propostos.copy()
        # Formatar para exibição
        df_proposto['data_lancamento_dt'] = pd.to_datetime(df_proposto['data_lancamento'])
        df_proposto['data_lancamento'] = df_proposto['data_lancamento_dt'].dt.strftime('%d/%m/%Y')
        df_proposto['valor_formatado'] = df_proposto['valor'].apply(formatar_moeda)
        
        st.dataframe(df_proposto[['data_lancamento', 'historico', 'valor_formatado', 'reduz_deb', 'nome_conta_d', 'reduz_cred', 'nome_conta_c', 'origem']], width='stretch')
        
        if st.button("✅ Salvar Lançamentos de Ajuste na Contabilidade"):
            try:
                partidas_para_salvar = st.session_state.lancamentos_negativos_propostos.to_dict('records')
                if salvar_partidas_lancamento(partidas_para_salvar):
                    st.success(f"{len(partidas_para_salvar)} lançamentos de ajuste salvos com sucesso!")
                    # Limpa o estado para não mostrar mais os lançamentos após salvar
                    st.session_state.lancamentos_negativos_propostos = pd.DataFrame()
                    st.rerun()
                else:
                    st.error("Ocorreu um erro ao tentar salvar os lançamentos no banco de dados.")
            except Exception as e:
                st.error(f"Ocorreu um erro ao salvar: {e}")


def submenu_analise_diferenca_debito_credito():
    """4.3 - Analisa lançamentos com diferença entre débito e crédito."""
    st.subheader("4.3 Análise de Diferença Débito/Crédito")

    st.markdown("""
    Esta ferramenta analisa os lançamentos contábeis salvos e identifica aqueles onde
    o **total de débitos** é diferente do **total de créditos** dentro do mesmo lançamento.

    Isso pode indicar:
    - Lançamentos incompletos
    - Erros de digitação
    - Problemas na importação dos dados
    """)

    # Carregar lançamentos
    df_lancamentos = carregar_lancamentos_contabeis()

    if df_lancamentos.empty:
        st.warning("Não há lançamentos contábeis cadastrados. Importe os lançamentos no Item 3.")
        return

    # Filtro por período
    st.markdown("##### Filtros")
    col1, col2 = st.columns(2)
    with col1:
        data_inicio_str = st.text_input("Data Início (DD/MM/YYYY)", value="01/01/2025", key="diff_data_inicio")
    with col2:
        data_fim_str = st.text_input("Data Fim (DD/MM/YYYY)", value=datetime.datetime.now().strftime("%d/%m/%Y"), key="diff_data_fim")

    if st.button("🔍 Analisar Lançamentos", type="primary"):
        with st.spinner("Analisando lançamentos..."):
            try:
                # Converter datas
                data_inicio = datetime.datetime.strptime(data_inicio_str, "%d/%m/%Y").date()
                data_fim = datetime.datetime.strptime(data_fim_str, "%d/%m/%Y").date()

                # Converter coluna de data
                df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'], errors='coerce')

                # Filtrar por período
                mask = (df_lancamentos['data_lancamento'].dt.date >= data_inicio) & \
                       (df_lancamentos['data_lancamento'].dt.date <= data_fim)
                df_filtrado = df_lancamentos[mask].copy()

                if df_filtrado.empty:
                    st.warning("Nenhum lançamento encontrado no período selecionado.")
                    return

                st.info(f"Total de registros no período: {len(df_filtrado)}")

                # Analisar por ID de lançamento
                lancamentos_com_diferenca = []

                for id_lanc in df_filtrado['idlancamento'].dropna().unique():
                    grupo = df_filtrado[df_filtrado['idlancamento'] == id_lanc]

                    # Calcular total de débitos (onde reduz_deb não é nulo)
                    debitos = grupo[pd.notna(grupo['reduz_deb'])]
                    total_debito = debitos['valor'].sum() if not debitos.empty else 0

                    # Calcular total de créditos (onde reduz_cred não é nulo)
                    creditos = grupo[pd.notna(grupo['reduz_cred'])]
                    total_credito = creditos['valor'].sum() if not creditos.empty else 0

                    # Verificar diferença (tolerância de 0.01 para erros de arredondamento)
                    diferenca = abs(total_debito - total_credito)
                    if diferenca > 0.01:
                        lancamentos_com_diferenca.append({
                            'ID Lançamento': id_lanc,
                            'Data': grupo['data_lancamento'].iloc[0].strftime('%d/%m/%Y') if pd.notna(grupo['data_lancamento'].iloc[0]) else '',
                            'Total Débito': total_debito,
                            'Total Crédito': total_credito,
                            'Diferença': diferenca,
                            'Histórico': grupo['historico'].iloc[0][:80] if pd.notna(grupo['historico'].iloc[0]) else '',
                            'Conta Deb': grupo['reduz_deb'].dropna().iloc[0] if not grupo['reduz_deb'].dropna().empty else '',
                            'Conta Cred': grupo['reduz_cred'].dropna().iloc[0] if not grupo['reduz_cred'].dropna().empty else ''
                        })

                # Exibir resultados
                if lancamentos_com_diferenca:
                    st.error(f"⚠️ Encontrados **{len(lancamentos_com_diferenca)}** lançamentos com diferença entre débito e crédito!")

                    df_resultado = pd.DataFrame(lancamentos_com_diferenca)

                    # Formatar valores para exibição
                    df_resultado['Total Débito'] = df_resultado['Total Débito'].apply(lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'))
                    df_resultado['Total Crédito'] = df_resultado['Total Crédito'].apply(lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'))
                    df_resultado['Diferença'] = df_resultado['Diferença'].apply(lambda x: f"R$ {x:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'))

                    st.dataframe(df_resultado, use_container_width=True)

                    # Estatísticas
                    st.markdown("---")
                    st.markdown("##### Resumo")
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total de Lançamentos", len(lancamentos_com_diferenca))
                    with col2:
                        total_dif = sum([l['Diferença'] if isinstance(l['Diferença'], (int, float)) else float(l['Diferença'].replace('R$ ', '').replace('.', '').replace(',', '.')) for l in lancamentos_com_diferenca])
                        st.metric("Soma das Diferenças", f"R$ {total_dif:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.'))
                    with col3:
                        # Contas mais frequentes
                        contas_deb = [l['Conta Deb'] for l in lancamentos_com_diferenca if l['Conta Deb']]
                        if contas_deb:
                            conta_freq = max(set(contas_deb), key=contas_deb.count)
                            st.metric("Conta Débito Mais Frequente", int(float(conta_freq)) if conta_freq else "-")

                    # Botão para download
                    df_download = pd.DataFrame(lancamentos_com_diferenca)
                    csv = df_download.to_csv(index=False, sep=';', decimal=',').encode('utf-8-sig')
                    st.download_button(
                        label="📥 Baixar lista (CSV)",
                        data=csv,
                        file_name="lancamentos_diferenca_debito_credito.csv",
                        mime="text/csv"
                    )

                else:
                    st.success("✅ Todos os lançamentos estão equilibrados (débito = crédito)!")

            except ValueError:
                st.error("Formato de data inválido. Use DD/MM/YYYY.")
            except Exception as e:
                st.error(f"Erro ao analisar lançamentos: {e}")
                st.exception(e)


def submenu_conciliacao_contas_contabeis_banco():
    """4.4 - Conciliação de Contas Contábeis de Banco (saldos negativos baseado em lançamentos contábeis)."""
    st.subheader("4.4 Conciliação Contas Contábeis de Banco")

    st.markdown("""
    Esta ferramenta analisa o saldo diário de uma conta bancária (baseado nos **lançamentos contábeis** importados)
    e gera lançamentos de ajuste automáticos para cobrir saldos negativos (credores).

    **Diferença do item 4.2:** O item 4.2 usa os extratos bancários OFX para calcular o saldo.
    Este item usa os **lançamentos contábeis** para calcular o saldo da conta.

    As contas contábeis são obtidas do **Cadastro de Contas Bancárias (Menu 1.1)**.
    """)

    # --- Seleção de Conta Bancária do Cadastro ---
    df_contas = carregar_cadastro_contas()
    if df_contas.empty:
        st.warning("O Cadastro de Contas (Menu 1.1) está vazio. É necessário cadastrar as contas primeiro.")
        return

    df_contas['Display'] = df_contas['Agencia'].astype(str) + " / " + df_contas['Conta'].astype(str)
    contas_display = df_contas[['Display']].drop_duplicates().sort_values('Display')

    col1, col2, col3 = st.columns([2, 1, 1])
    with col1:
        conta_selecionada_display = st.selectbox(
            "Selecione a Conta Bancária para análise:",
            options=contas_display['Display'].tolist(),
            key="ccb_conta_select"
        )

    today = datetime.date.today()
    first_day_of_month = today.replace(day=1)
    last_month = first_day_of_month - datetime.timedelta(days=1)

    with col2:
        data_inicio_str = st.text_input(
            "Data de Início da Análise (DD/MM/AAAA)",
            value=last_month.replace(day=1).strftime('%d/%m/%Y'),
            key="ccb_data_inicio"
        )
    with col3:
        data_fim_str = st.text_input(
            "Data Final da Análise (DD/MM/AAAA)",
            value=last_month.strftime('%d/%m/%Y'),
            key="ccb_data_fim"
        )

    # Inicializa o estado para os lançamentos propostos
    if 'lancamentos_contabeis_negativos_propostos' not in st.session_state:
        st.session_state.lancamentos_contabeis_negativos_propostos = pd.DataFrame()

    # --- Botão de Análise ---
    if st.button("🔍 Analisar Saldo Negativo Contábil", type="primary"):
        try:
            data_inicio = datetime.datetime.strptime(data_inicio_str, '%d/%m/%Y').date()
            data_fim = datetime.datetime.strptime(data_fim_str, '%d/%m/%Y').date()

            conta_selecionada_row = df_contas[df_contas['Display'] == conta_selecionada_display].iloc[0]

            with st.spinner("Analisando saldos contábeis e gerando lançamentos..."):
                lancamentos_propostos_df = gerar_lancamentos_saldo_negativo_contabil_cadastro(
                    conta_selecionada_row, data_inicio, data_fim
                )
                st.session_state.lancamentos_contabeis_negativos_propostos = lancamentos_propostos_df

        except ValueError:
            st.error("Formato de data inválido. Por favor, use DD/MM/AAAA.")
            st.session_state.lancamentos_contabeis_negativos_propostos = pd.DataFrame()
        except Exception as e:
            st.error(f"Ocorreu um erro inesperado durante a análise: {e}")
            st.session_state.lancamentos_contabeis_negativos_propostos = pd.DataFrame()

    # --- Exibição dos Lançamentos Propostos e Botão de Salvar ---
    if not st.session_state.lancamentos_contabeis_negativos_propostos.empty:
        st.markdown("---")
        st.subheader("Lançamentos de Ajuste Propostos")

        df_proposto = st.session_state.lancamentos_contabeis_negativos_propostos.copy()
        # Formatar para exibição
        df_proposto['data_lancamento_dt'] = pd.to_datetime(df_proposto['data_lancamento'])
        df_proposto['data_lancamento'] = df_proposto['data_lancamento_dt'].dt.strftime('%d/%m/%Y')
        df_proposto['valor_formatado'] = df_proposto['valor'].apply(formatar_moeda)

        st.dataframe(
            df_proposto[['data_lancamento', 'historico', 'valor_formatado', 'reduz_deb', 'nome_conta_d', 'reduz_cred', 'nome_conta_c', 'origem']],
            use_container_width=True
        )

        if st.button("✅ Salvar Lançamentos de Ajuste na Contabilidade"):
            try:
                partidas_para_salvar = st.session_state.lancamentos_contabeis_negativos_propostos.to_dict('records')
                if salvar_partidas_lancamento(partidas_para_salvar):
                    st.success(f"{len(partidas_para_salvar)} lançamentos de ajuste salvos com sucesso!")
                    # Limpa o estado para não mostrar mais os lançamentos após salvar
                    st.session_state.lancamentos_contabeis_negativos_propostos = pd.DataFrame()
                    st.rerun()
                else:
                    st.error("Ocorreu um erro ao tentar salvar os lançamentos no banco de dados.")
            except Exception as e:
                st.error(f"Ocorreu um erro ao salvar: {e}")


def main():
    st.set_page_config(layout="wide", page_title="Sistema de Conciliação Bancária")
    
    # Injetar CSS para diminuir a fonte dos totalizadores
    st.markdown(r'''
        <style>
        [data-testid="stMetricValue"] {
            font-size: 1.75rem;
        }
        </style>
    ''', unsafe_allow_html=True)

    init_db()

    # Inicializa session_state para controle de tela
    if 'tela_atual' not in st.session_state:
        st.session_state.tela_atual = None

    # Controle de navegação entre telas
    if st.session_state.tela_atual == "cadastro_empresa":
        tela_cadastro_empresa()
        return  # Para aqui e não exibe o resto
    elif st.session_state.tela_atual == "gerenciar_socios":
        tela_gerenciar_socios()
        return  # Para aqui e não exibe o resto
    elif st.session_state.tela_atual == "gerenciar_logotipos":
        tela_gerenciar_logotipos()
        return  # Para aqui e não exibe o resto

    # Caso contrário, exibe a interface normal
    st.title("Sistema de Conciliação Bancária")

    # Botão de cadastro da empresa no sidebar
    sidebar_botao_cadastro_empresa()

    # Opções de Reset
    with st.sidebar.expander("⚠️ Opções de Reset", expanded=False):
        if st.button("Resetar Banco de Dados"):
            db_file = 'conciliacao_db.sqlite'
            if os.path.exists(db_file):
                os.remove(db_file)
                st.success("Banco de dados resetado.")
                st.rerun()

    st.sidebar.title("Menu Principal")
    menu_option = st.sidebar.selectbox(
        "Selecione a seção:",
        ["1. Cadastro", "2. Extrato Bancário", "3. Extrato Lançamento", "4. Lançamentos Contábeis", "5. Conciliação", "6. Relatórios", "7. Exportação", "8. Parcelamentos"],
        key="menu_principal_selectbox"
    )

    # Limpa session_states de parcelamentos quando sair do menu 8
    if menu_option != "8. Parcelamentos":
        for key in ['parcelamento_selecionado', 'parcelamento_editar']:
            if key in st.session_state:
                del st.session_state[key]

    # Botão de limpar cache no sidebar
    st.sidebar.markdown("---")
    if st.sidebar.button("🔄 Limpar Cache", help="Limpa o cache e atualiza todos os dados", use_container_width=True):
        st.cache_data.clear()
        st.sidebar.success("✅ Cache limpo!")
        st.rerun()
    st.sidebar.caption("💡 Use se os dados não estiverem atualizados")
    st.sidebar.markdown("---")

    df_bancos = ler_bancos_associados()

    if menu_option == "1. Cadastro":
        st.subheader("1. Cadastros Gerais")
        sub_menu_option = st.selectbox("Selecione a Ação:", ["1.1 Cadastro de Contas Bancarias", "1.2 Cadastro de contas Contabeis"])
        if sub_menu_option == "1.1 Cadastro de Contas Bancarias":
            st.subheader("1.1 Cadastro de Contas Bancarias (Agência/Conta)")
            df_contas = carregar_cadastro_contas()
            df_bancos = ler_bancos_associados()

            # --- Seção de Upload de Logos para a pasta do projeto ---
            with st.expander("Adicionar/Atualizar Logos na Pasta do Projeto"):
                st.markdown("""
                **Passo 1:** Envie os arquivos de imagem dos logos aqui.
                **Passo 2:** Na tabela abaixo, na coluna `Path_Logo`, digite o caminho completo, como `logos/nomedoarquivo.png`.
                """)
                uploaded_logos = st.file_uploader(
                    "Selecione um ou mais arquivos de logo", 
                    type=['png', 'jpg', 'jpeg'], 
                    accept_multiple_files=True,
                    key="logos_uploader"
                )
                if uploaded_logos:
                    saved_files = []
                    for logo in uploaded_logos:
                        caminho_salvar = os.path.join('logos', logo.name)
                        with open(caminho_salvar, "wb") as f:
                            f.write(logo.getbuffer())
                        saved_files.append(logo.name)
                    st.success(f"Logos salvos na pasta 'logos': {', '.join(saved_files)}")

            # --- Seção de Importação de Cadastro ---
            with st.expander("📥 Importar Cadastro de Contas (Excel/CSV)"):
                if 'processed_file_id' not in st.session_state:
                    st.session_state.processed_file_id = None

                uploaded_file = st.file_uploader("Selecione o arquivo de Cadastro", type=['xlsx', 'csv', 'xls'], key="upload_cadastro_contas")
                
                # Process the file only if it's new
                if uploaded_file is not None:
                    current_file_id = f"{uploaded_file.name}-{uploaded_file.size}"
                    if current_file_id != st.session_state.get('processed_file_id'):
                        df_temp = ler_cadastro_contas(uploaded_file)
                        if not df_temp.empty:
                            df_temp['Codigo_Banco'] = df_temp['Codigo_Banco'].astype(str)
                            df_bancos['codigo_banco'] = df_bancos['codigo_banco'].astype(str)
                            df_temp = pd.merge(df_temp, df_bancos[['codigo_banco', 'Path_Logo']], left_on='Codigo_Banco', right_on='codigo_banco', how='left', suffixes=('_old', ''))
                            if 'Path_Logo_old' in df_temp.columns:
                                df_temp['Path_Logo'].fillna(df_temp['Path_Logo_old'], inplace=True)
                            df_temp['Path_Logo'] = df_temp['Path_Logo'].fillna(os.path.join('logos', 'default.png'))
                            df_temp.drop(columns=['Path_Logo_old', 'codigo_banco'], errors='ignore', inplace=True)
                            
                            salvar_cadastro_contas(df_temp)
                            
                            # Mark the file as processed and rerun
                            st.session_state.processed_file_id = current_file_id
                            st.success("Arquivo de cadastro importado e salvo!")
                            st.rerun()

            st.markdown("---")
            st.subheader("Edição de Contas Bancárias")
            st.info("Para associar um logo, primeiro envie o arquivo no expansor 'Adicionar/Atualizar Logos' acima, depois digite o caminho (ex: `logos/nomelogo.png`) na coluna `Path_Logo` da tabela.")
            st.info("Para **excluir linhas**, selecione as linhas clicando nas caixas de seleção que aparecem à esquerda e pressione a tecla 'Delete' no seu teclado. Para **adicionar uma nova linha**, role até o final da tabela e clique no '+'.")
            
            edited_df = st.data_editor(
                df_contas,
                num_rows="dynamic",
                width='stretch',
                key="editor_cadastro_contas",
                column_config={
                    "Path_Logo": st.column_config.TextColumn(
                        "Caminho do Logo (ex: logos/banco.png)",
                        help="Digite o caminho para o logo. Ex: logos/nomelogo.png",
                        width="medium"
                    )
                }
            )
            if st.button("✏️ Salvar Edições/Exclusões"):
                salvar_cadastro_contas(pd.DataFrame(edited_df))
                st.success("Alterações salvas!")
                st.rerun()
        elif sub_menu_option == "1.2 Cadastro de contas Contabeis":
            submenu_plano_contas()

    elif menu_option == "2. Extrato Bancário":
        st.subheader("2. Extrato Bancário")
        sub_menu_option = st.selectbox("Selecione a Ação:", ["2.1 Importação de Extrato (OFX)", "2.2 Visualização de Extrato Salvo"])
        if sub_menu_option == "2.1 Importação de Extrato (OFX)":
            submenu_extrato_importacao(df_bancos)
        elif sub_menu_option == "2.2 Visualização de Extrato Salvo":
            submenu_extrato_visualizacao()

    elif menu_option == "3. Extrato Lançamento":
        st.subheader("3. Upload Extrato Lançamento Contábil")

        st.info("""
        **Formato esperado do arquivo:**
        - Colunas: Data, Valor, Historico (ou Descricao), ReduzDeb, NomeContaD, ReduzCred, NomeContaC
        - Formatos aceitos: Excel (.xlsx, .xls) ou CSV (separador ;)
        """)

        uploaded_file = st.file_uploader("Selecione o arquivo Contábil", type=['xlsx', 'xls', 'csv'])
        substituir_dados = st.checkbox("Substituir lançamentos existentes", value=False)

        if uploaded_file:
            df_contabil = ler_extrato_contabil(uploaded_file)

            # Verificar se leitura funcionou
            if df_contabil.empty:
                st.error("Nenhum dado foi lido do arquivo. Verifique o formato e as colunas.")
                st.stop()

            st.success(f"Arquivo lido com sucesso: {len(df_contabil)} lançamentos encontrados")
            st.write(f"**Colunas detectadas:** {list(df_contabil.columns)}")

            # Só limpa se checkbox marcado E arquivo foi lido com sucesso
            if substituir_dados:
                limpar_lancamentos_contabeis()

            # Adicionar colunas de origem
            df_contabil['Origem'] = 'Sistema Origem'

            # =====================================================
            # VALIDAÇÃO: Detectar lançamentos com DÉBITO = CRÉDITO
            # =====================================================
            if 'ID Lancamento' in df_contabil.columns and 'ReduzDeb' in df_contabil.columns and 'ReduzCred' in df_contabil.columns:
                lancamentos_problematicos = []

                for id_lanc in df_contabil['ID Lancamento'].dropna().unique():
                    grupo = df_contabil[df_contabil['ID Lancamento'] == id_lanc]

                    # Pegar contas reduzidas de débito e crédito
                    reduz_deb = grupo['ReduzDeb'].dropna().unique()
                    reduz_cred = grupo['ReduzCred'].dropna().unique()

                    # Se tem 1 débito e 1 crédito e são iguais = problema
                    if len(reduz_deb) == 1 and len(reduz_cred) == 1:
                        try:
                            deb_val = int(float(reduz_deb[0]))
                            cred_val = int(float(reduz_cred[0]))
                            if deb_val == cred_val:
                                lancamentos_problematicos.append({
                                    'ID Lançamento': int(id_lanc),
                                    'Conta': deb_val,
                                    'Valor': grupo['Valor'].iloc[0] if 'Valor' in grupo.columns else 0
                                })
                        except (ValueError, TypeError):
                            pass

                if lancamentos_problematicos:
                    st.warning(f"⚠️ **ATENÇÃO:** Foram encontrados {len(lancamentos_problematicos)} lançamentos com DÉBITO = CRÉDITO na mesma conta!")
                    st.info("Esses lançamentos têm a mesma conta reduzida tanto no débito quanto no crédito, o que pode indicar erro nos dados de origem.")

                    with st.expander(f"Ver {len(lancamentos_problematicos)} lançamentos problemáticos"):
                        df_problemas = pd.DataFrame(lancamentos_problematicos)
                        st.dataframe(df_problemas)

                        # Estatísticas por conta
                        st.markdown("**Resumo por conta:**")
                        resumo = df_problemas.groupby('Conta').size().reset_index(name='Quantidade')
                        st.dataframe(resumo)

                        # Botão para baixar CSV dos problemáticos
                        csv_problemas = df_problemas.to_csv(index=False, sep=';').encode('utf-8-sig')
                        st.download_button(
                            label="📥 Baixar lista de lançamentos problemáticos (CSV)",
                            data=csv_problemas,
                            file_name="lancamentos_debito_igual_credito.csv",
                            mime="text/csv"
                        )

            salvar_lancamentos_contabeis(df_contabil)
            st.success("Lançamentos contábeis importados e salvos.")
            st.dataframe(df_contabil.head())

    elif menu_option == "4. Lançamentos Contábeis":
        st.subheader("4. Lançamentos Contábeis")
        sub_menu_4 = st.selectbox("Selecione a Ação:", ["4.0 Visualizar Lançamentos", "4.1 Adicionar Lançamento", "4.2 Conciliacao Contas Negativas", "4.3 Análise Diferença Débito/Crédito", "4.4 Conciliação Contas Contábeis Banco"])

        if sub_menu_4 == "4.0 Visualizar Lançamentos":
            submenu_lancamentos_contabeis_visualizacao()
        elif sub_menu_4 == "4.1 Adicionar Lançamento":
            submenu_lancamentos_contabeis_adicionar()
        elif sub_menu_4 == "4.2 Conciliacao Contas Negativas":
            submenu_conciliacao_contas_negativas()
        elif sub_menu_4 == "4.3 Análise Diferença Débito/Crédito":
            submenu_analise_diferenca_debito_credito()
        elif sub_menu_4 == "4.4 Conciliação Contas Contábeis Banco":
            submenu_conciliacao_contas_contabeis_banco()

    elif menu_option == "5. Conciliação":
        st.subheader("5. Conciliação")
        sub_menu_5 = st.selectbox("Selecione a Ação:", ["5.1 Conciliação Banco x Contábil"])

        if sub_menu_5 == "5.1 Conciliação Banco x Contábil":
            submenu_conciliacao_banco_contabil()

    elif menu_option == "6. Relatórios":
        st.subheader("6. Relatórios")
        sub_menu_6 = st.selectbox("Selecione a Ação:", [
            "6.1 Extratos Bancários",
            "6.2 Balancete de Verificação",
            "6.3 Livro Diário",
            "6.4 Livro Razão",
            "6.5 Balanço Patrimonial"
        ])

        if sub_menu_6 == "6.1 Extratos Bancários":
            submenu_relatorios_extratos_bancarios()
        elif sub_menu_6 == "6.2 Balancete de Verificação":
            submenu_relatorio_balancete()
        elif sub_menu_6 == "6.3 Livro Diário":
            submenu_relatorio_livro_diario()
        elif sub_menu_6 == "6.4 Livro Razão":
            submenu_relatorio_livro_razao()
        elif sub_menu_6 == "6.5 Balanço Patrimonial":
            submenu_relatorio_balanco_patrimonial()

    elif menu_option == "7. Exportação":
        st.subheader("7. Exportação")
        sub_menu_7 = st.selectbox("Selecione a Ação:", [
            "7.1 Domínio Sistemas",
            "7.2 Relatórios Excel"
        ])

        if sub_menu_7 == "7.1 Domínio Sistemas":
            submenu_exportacao_dominio()
        elif sub_menu_7 == "7.2 Relatórios Excel":
            submenu_exportacao_relatorios_excel()

    elif menu_option == "8. Parcelamentos":
        st.subheader("8. Parcelamentos Tributários")

        # Verifica se há parcelamento selecionado/editando ANTES do selectbox
        if st.session_state.get('parcelamento_selecionado'):
            parcelamento_id = st.session_state['parcelamento_selecionado']
            exibir_detalhes_parcelamento(parcelamento_id)
        elif st.session_state.get('parcelamento_editar'):
            parcelamento_id = st.session_state['parcelamento_editar']
            exibir_formulario_edicao_parcelamento(parcelamento_id)
        else:
            # Só mostra o submenu se não houver parcelamento selecionado/editando
            sub_menu_8 = st.selectbox("Selecione a Ação:", [
                "8.1 Cadastro de Parcelamentos",
                "8.2 Importar PDF e-CAC",
                "8.3 Controle de Parcelas",
                "8.4 Conciliação com Extrato",
                "8.5 Lançamentos Contábeis"
            ], key="sub_menu_8_selectbox")

            if sub_menu_8 == "8.1 Cadastro de Parcelamentos":
                submenu_parcelamentos_cadastro()
            elif sub_menu_8 == "8.2 Importar PDF e-CAC":
                submenu_parcelamentos_importar_pdf()
            elif sub_menu_8 == "8.3 Controle de Parcelas":
                submenu_parcelamentos_controle_parcelas()
            elif sub_menu_8 == "8.4 Conciliação com Extrato":
                submenu_parcelamentos_conciliacao()
            elif sub_menu_8 == "8.5 Lançamentos Contábeis":
                submenu_parcelamentos_lancamentos()

def submenu_exportacao_dominio():
    """Exporta lançamentos contábeis no formato Domínio Sistemas - Layout Lançamentos em Lote."""
    st.subheader("7.1 Exportação Domínio Sistemas")
    st.markdown("Exporte os lançamentos contábeis no formato do sistema Domínio (Lançamentos em Lote).")

    # Carregar dados da empresa
    from db_manager import carregar_empresa

    empresa = carregar_empresa()
    if not empresa:
        st.warning("⚠️ Por favor, cadastre os dados da empresa primeiro.")
        return

    # CGC/CNPJ com 14 dígitos (sem pontuação)
    cnpj = empresa.get('cnpj', '').replace('.', '').replace('/', '').replace('-', '')
    cnpj = cnpj.zfill(14)  # Garante 14 dígitos

    # Filtros
    st.markdown("##### Filtros")

    # Seleção de período
    col1, col2 = st.columns(2)
    with col1:
        data_inicio_str = st.text_input("Data Início (DD/MM/YYYY)", value="01/01/2025", key="dominio_data_inicio")
    with col2:
        data_fim_str = st.text_input("Data Fim (DD/MM/YYYY)", value=datetime.datetime.now().strftime("%d/%m/%Y"), key="dominio_data_fim")

    # Filtro de origem
    origens_lancamento = st.multiselect(
        "Origem do Lançamento",
        options=["Manual", "conta negativa", "Sistema Origem"],
        default=["Manual", "conta negativa", "Sistema Origem"],
        key="dominio_origem_lancamento"
    )

    # Sub-filtro de tipo de lançamento (apenas para origem Manual)
    tipo_lancamento_manual = None
    if "Manual" in origens_lancamento:
        tipo_lancamento_manual = st.multiselect(
            "Tipo de Lançamento Manual",
            options=["Inclusão", "Baixa"],
            default=["Inclusão", "Baixa"],
            key="dominio_tipo_manual"
        )

    # Código da empresa no Domínio (7 dígitos)
    codigo_empresa = st.text_input("Código da Empresa no Domínio", value="0000561", max_chars=7)

    if st.button("📥 Gerar Arquivo Domínio", type="primary"):
        with st.spinner("Gerando arquivo..."):
            try:
                from db_manager import carregar_lancamentos_contabeis

                # Validar datas
                try:
                    data_inicio = datetime.datetime.strptime(data_inicio_str, "%d/%m/%Y")
                    data_fim = datetime.datetime.strptime(data_fim_str, "%d/%m/%Y")
                except ValueError:
                    st.error("⚠️ Formato de data inválido. Use DD/MM/YYYY.")
                    return

                # Validar filtro de origem
                if not origens_lancamento:
                    st.warning("⚠️ Selecione pelo menos uma origem de lançamento.")
                    return

                # Carregar lançamentos
                df_lancamentos = carregar_lancamentos_contabeis()

                if df_lancamentos.empty:
                    st.warning("⚠️ Não há lançamentos contábeis para exportar.")
                    return

                total_inicial = len(df_lancamentos)
                st.info(f"Total de lançamentos carregados: {total_inicial}")

                # Filtrar por origem
                df_lancamentos = df_lancamentos[df_lancamentos['origem'].isin(origens_lancamento)]
                st.info(f"Lançamentos após filtro de origem: {len(df_lancamentos)}")

                # Se origem Manual foi selecionada e há filtro de tipo, aplicar filtro adicional
                if "Manual" in origens_lancamento and tipo_lancamento_manual:
                    # Pegar apenas lançamentos manuais com os tipos selecionados
                    df_manuais = df_lancamentos[df_lancamentos['origem'] == 'Manual']
                    df_manuais = df_manuais[df_manuais['tipo_lancamento'].isin(tipo_lancamento_manual)]

                    # Pegar lançamentos de outras origens
                    df_outras_origens = df_lancamentos[df_lancamentos['origem'] != 'Manual']

                    # Combinar
                    df_lancamentos = pd.concat([df_manuais, df_outras_origens], ignore_index=True)
                    st.info(f"Lançamentos após filtro de tipo manual: {len(df_lancamentos)}")

                # Filtrar por período
                df_lancamentos['data_lancamento'] = pd.to_datetime(df_lancamentos['data_lancamento'])
                df_lancamentos = df_lancamentos[
                    (df_lancamentos['data_lancamento'] >= data_inicio) &
                    (df_lancamentos['data_lancamento'] <= data_fim)
                ]
                st.info(f"Lançamentos após filtro de período: {len(df_lancamentos)}")

                if df_lancamentos.empty:
                    st.warning("⚠️ Não há lançamentos no período selecionado com os filtros aplicados.")
                    return

                # Agrupar por idlancamento e data
                grupos = df_lancamentos.groupby(['idlancamento', df_lancamentos['data_lancamento'].dt.date])

                linhas = []
                seq_geral = 1

                # Código da filial (sempre 0 conforme solicitado)
                codigo_filial = "0000000"  # 7 dígitos

                # Usuário (30 caracteres - pode ser vazio)
                usuario = " " * 30

                # Processar cada grupo (lote de lançamento)
                for (idlancamento, data_lanc), grupo in grupos:
                    # Separar débitos e créditos (baseado em reduz_deb e reduz_cred)
                    debitos = grupo[pd.notna(grupo['reduz_deb']) & (grupo['reduz_deb'] != '')].copy()
                    creditos = grupo[pd.notna(grupo['reduz_cred']) & (grupo['reduz_cred'] != '')].copy()

                    num_debitos = len(debitos)
                    num_creditos = len(creditos)

                    # Determinar tipo de lançamento conforme layout Domínio
                    # D = Um débito para vários créditos
                    # C = Um crédito para vários débitos
                    # X = Um débito para um crédito
                    # V = Vários débitos para vários créditos
                    if num_debitos == 1 and num_creditos == 1:
                        tipo_lanc = 'X'  # Um débito para um crédito
                    elif num_debitos == 1 and num_creditos > 1:
                        tipo_lanc = 'D'  # Um débito para vários créditos
                    elif num_debitos > 1 and num_creditos == 1:
                        tipo_lanc = 'C'  # Um crédito para vários débitos
                    elif num_debitos > 1 and num_creditos > 1:
                        tipo_lanc = 'V'  # Vários débitos para vários créditos
                    else:
                        continue  # Pular se não houver débitos ou créditos

                    # Pegar histórico (usar o primeiro registro do grupo)
                    historico_raw = grupo.iloc[0]['historico'] if pd.notna(grupo.iloc[0]['historico']) else ''
                    historico_complemento = str(historico_raw)[:512].ljust(512)  # 512 caracteres

                    # Data do lançamento (DD/MM/YYYY)
                    data_str = data_lanc.strftime('%d/%m/%Y')

                    # ============================================================
                    # LINHA TIPO 02 - Identificação do Lote (150 caracteres)
                    # ============================================================
                    # Posição 001-002: "02" (fixo)
                    # Posição 003-009: Sequencial (7 dígitos)
                    # Posição 010-010: Tipo (D/C/X/V)
                    # Posição 011-020: Data (DD/MM/YYYY)
                    # Posição 021-050: Usuário (30 caracteres)
                    # Posição 051-150: Brancos (100 caracteres)
                    linha_02 = f"02{seq_geral:07d}{tipo_lanc}{data_str}{usuario}{' ' * 100}"
                    linhas.append(linha_02)
                    seq_geral += 1

                    # ============================================================
                    # LINHAS TIPO 03 - Lançamentos Contábeis (664 caracteres)
                    # ============================================================
                    # Para lançamentos X (1 débito, 1 crédito): gera 1 linha com débito e crédito
                    # Para outros tipos: gera 1 linha por partida

                    if tipo_lanc == 'X':
                        # Lançamento simples: 1 débito e 1 crédito na mesma linha
                        deb = debitos.iloc[0]
                        cred = creditos.iloc[0]

                        # Converter para inteiro primeiro para remover decimais (.0)
                        conta_deb_raw = deb.get('reduz_deb', '')
                        try:
                            conta_deb = str(int(float(conta_deb_raw))).zfill(7)[:7]
                        except (ValueError, TypeError):
                            conta_deb = str(conta_deb_raw).replace('.', '').replace('-', '').zfill(7)[:7]

                        conta_cred_raw = cred.get('reduz_cred', '')
                        try:
                            conta_cred = str(int(float(conta_cred_raw))).zfill(7)[:7]
                        except (ValueError, TypeError):
                            conta_cred = str(conta_cred_raw).replace('.', '').replace('-', '').zfill(7)[:7]

                        # Valor com 2 decimais, 15 posições (13 inteiros + 2 decimais, sem separador)
                        valor = abs(float(deb.get('valor', 0)))
                        valor_str = f"{valor * 100:015.0f}"  # Centavos, 15 dígitos

                        # Código do histórico (7 dígitos) - usar 0 se não houver
                        cod_historico = "0000000"

                        # Linha 03: Posições conforme layout
                        # 001-002: "03"
                        # 003-009: Sequencial (7)
                        # 010-016: Conta Débito (7)
                        # 017-023: Conta Crédito (7)
                        # 024-038: Valor (15)
                        # 039-045: Código Histórico (7)
                        # 046-557: Histórico Complemento (512)
                        # 558-564: Código Filial (7) - sempre 0
                        # 565-664: Brancos (100)
                        linha_03 = f"03{seq_geral:07d}{conta_deb}{conta_cred}{valor_str}{cod_historico}{historico_complemento}{codigo_filial}{' ' * 100}"
                        linhas.append(linha_03)
                        seq_geral += 1
                    else:
                        # Lançamentos compostos: uma linha por partida
                        # IMPORTANTE: A ordem das partidas depende do tipo do lote!
                        # Tipo C (Um crédito para vários débitos): CRÉDITO primeiro, depois débitos
                        # Tipo D (Um débito para vários créditos): DÉBITO primeiro, depois créditos
                        # Tipo V (Vários para vários): Débitos primeiro, depois créditos

                        if tipo_lanc == 'C':
                            # Tipo C: Primeiro o CRÉDITO (único), depois os débitos
                            # Crédito primeiro
                            for _, cred in creditos.iterrows():
                                conta_deb = "0000000"  # Débito zerado
                                conta_cred_raw = cred.get('reduz_cred', '')
                                try:
                                    conta_cred = str(int(float(conta_cred_raw))).zfill(7)[:7]
                                except (ValueError, TypeError):
                                    conta_cred = str(conta_cred_raw).replace('.', '').replace('-', '').zfill(7)[:7]

                                valor = abs(float(cred.get('valor', 0)))
                                valor_str = f"{valor * 100:015.0f}"

                                cod_historico = "0000000"

                                linha_03 = f"03{seq_geral:07d}{conta_deb}{conta_cred}{valor_str}{cod_historico}{historico_complemento}{codigo_filial}{' ' * 100}"
                                linhas.append(linha_03)
                                seq_geral += 1

                            # Depois os débitos
                            for _, deb in debitos.iterrows():
                                conta_deb_raw = deb.get('reduz_deb', '')
                                try:
                                    conta_deb = str(int(float(conta_deb_raw))).zfill(7)[:7]
                                except (ValueError, TypeError):
                                    conta_deb = str(conta_deb_raw).replace('.', '').replace('-', '').zfill(7)[:7]
                                conta_cred = "0000000"  # Crédito zerado

                                valor = abs(float(deb.get('valor', 0)))
                                valor_str = f"{valor * 100:015.0f}"

                                cod_historico = "0000000"

                                linha_03 = f"03{seq_geral:07d}{conta_deb}{conta_cred}{valor_str}{cod_historico}{historico_complemento}{codigo_filial}{' ' * 100}"
                                linhas.append(linha_03)
                                seq_geral += 1

                        else:
                            # Tipo D ou V: Primeiro os DÉBITOS, depois os créditos
                            # Débitos primeiro
                            for _, deb in debitos.iterrows():
                                conta_deb_raw = deb.get('reduz_deb', '')
                                try:
                                    conta_deb = str(int(float(conta_deb_raw))).zfill(7)[:7]
                                except (ValueError, TypeError):
                                    conta_deb = str(conta_deb_raw).replace('.', '').replace('-', '').zfill(7)[:7]
                                conta_cred = "0000000"  # Crédito zerado

                                valor = abs(float(deb.get('valor', 0)))
                                valor_str = f"{valor * 100:015.0f}"

                                cod_historico = "0000000"

                                linha_03 = f"03{seq_geral:07d}{conta_deb}{conta_cred}{valor_str}{cod_historico}{historico_complemento}{codigo_filial}{' ' * 100}"
                                linhas.append(linha_03)
                                seq_geral += 1

                            # Depois os créditos
                            for _, cred in creditos.iterrows():
                                conta_deb = "0000000"  # Débito zerado
                                conta_cred_raw = cred.get('reduz_cred', '')
                                try:
                                    conta_cred = str(int(float(conta_cred_raw))).zfill(7)[:7]
                                except (ValueError, TypeError):
                                    conta_cred = str(conta_cred_raw).replace('.', '').replace('-', '').zfill(7)[:7]

                                valor = abs(float(cred.get('valor', 0)))
                                valor_str = f"{valor * 100:015.0f}"

                                cod_historico = "0000000"

                                linha_03 = f"03{seq_geral:07d}{conta_deb}{conta_cred}{valor_str}{cod_historico}{historico_complemento}{codigo_filial}{' ' * 100}"
                                linhas.append(linha_03)
                                seq_geral += 1

                st.info(f"Total de linhas geradas para exportação: {len(linhas)}")

                if not linhas:
                    st.warning("⚠️ Nenhum lançamento válido para exportar.")
                    st.info("Dica: Verifique se os lançamentos têm tanto débito (reduz_deb) quanto crédito (reduz_cred) preenchidos.")
                    return

                # ============================================================
                # LINHA TIPO 01 - Cabeçalho (54 caracteres)
                # ============================================================
                # Posição 001-002: "01" (fixo)
                # Posição 003-009: Código da Empresa (7 dígitos)
                # Posição 010-023: CGC/CNPJ (14 caracteres)
                # Posição 024-033: Data Inicial (DD/MM/YYYY)
                # Posição 034-043: Data Final (DD/MM/YYYY)
                # Posição 044-044: "N" (fixo)
                # Posição 045-046: Tipo de Nota "05" (Contabilidade-Lançamentos em lote)
                # Posição 047-051: Constante "00000"
                # Posição 052-052: Sistema "1" (Contabilidade)
                # Posição 053-054: "16" (fixo)
                data_ini_str = data_inicio.strftime('%d/%m/%Y')
                data_fim_formatada = data_fim.strftime('%d/%m/%Y')

                linha_01 = f"01{codigo_empresa.zfill(7)}{cnpj}{data_ini_str}{data_fim_formatada}N05000001016"

                # ============================================================
                # LINHA TIPO 99 - Finalizador (100 caracteres de "9")
                # ============================================================
                linha_99 = "9" * 100

                # Montar arquivo completo
                conteudo = linha_01 + "\n"
                for linha in linhas:
                    conteudo += linha + "\n"
                conteudo += linha_99 + "\n"

                # Preparar para download
                nome_arquivo = f"lancamentos_dominio_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.txt"

                st.success(f"✅ Arquivo gerado com sucesso! Total de {len(linhas)} registros de lançamentos.")
                st.download_button(
                    label="📥 Baixar Arquivo Domínio",
                    data=conteudo,
                    file_name=nome_arquivo,
                    mime="text/plain"
                )

            except Exception as e:
                st.error(f"Erro ao gerar arquivo: {e}")
                st.exception(e)

def submenu_exportacao_relatorios_excel():
    """Exporta relatórios contábeis em Excel."""
    st.subheader("7.2 Exportação de Relatórios em Excel")
    st.markdown("Exporte todos os relatórios contábeis em um único arquivo Excel.")

    # Seleção de período
    col1, col2 = st.columns(2)
    with col1:
        data_inicio_str = st.text_input("Data Início (DD/MM/YYYY)", value="01/01/2025", key="export_data_inicio")
    with col2:
        data_fim_str = st.text_input("Data Fim (DD/MM/YYYY)", value=datetime.datetime.now().strftime("%d/%m/%Y"), key="export_data_fim")

    # Seleção de relatórios
    st.markdown("### Selecione os relatórios para exportar:")

    col1, col2 = st.columns(2)
    with col1:
        incluir_balancete = st.checkbox("Balancete de Verificação", value=True)
        incluir_diario = st.checkbox("Livro Diário", value=True)
    with col2:
        incluir_razao = st.checkbox("Livro Razão", value=True)
        incluir_balanco = st.checkbox("Balanço Patrimonial", value=True)

    if st.button("📥 Gerar e Baixar Excel", type="primary"):
        with st.spinner("Gerando arquivo Excel..."):
            try:
                # Validar datas
                try:
                    data_inicio = datetime.datetime.strptime(data_inicio_str, "%d/%m/%Y")
                    data_fim = datetime.datetime.strptime(data_fim_str, "%d/%m/%Y")
                except ValueError:
                    st.error("⚠️ Formato de data inválido. Use DD/MM/YYYY.")
                    return

                # Importar biblioteca necessária
                from io import BytesIO
                from db_manager import carregar_lancamentos_contabeis

                # Criar arquivo Excel em memória
                output = BytesIO()
                writer = pd.ExcelWriter(output, engine='xlsxwriter')

                # Carregar dados necessários
                df_plano_contas = carregar_plano_contas()
                df_lancamentos = carregar_lancamentos_contabeis()

                # Filtrar por período
                if not df_lancamentos.empty:
                    df_lancamentos['data'] = pd.to_datetime(df_lancamentos['data'])
                    df_lancamentos = df_lancamentos[
                        (df_lancamentos['data'] >= data_inicio) &
                        (df_lancamentos['data'] <= data_fim)
                    ]

                # 1. Balancete de Verificação
                if incluir_balancete and not df_lancamentos.empty:
                    saldos = {}
                    for _, row in df_lancamentos.iterrows():
                        conta = row['conta']
                        valor_debito = row.get('valor_debito', 0) if pd.notna(row.get('valor_debito', 0)) else 0
                        valor_credito = row.get('valor_credito', 0) if pd.notna(row.get('valor_credito', 0)) else 0

                        if conta not in saldos:
                            saldos[conta] = {'debito': 0, 'credito': 0}

                        saldos[conta]['debito'] += valor_debito
                        saldos[conta]['credito'] += valor_credito

                    balancete_data = []
                    for conta, valores in sorted(saldos.items()):
                        if conta in df_plano_contas['codigo'].values:
                            conta_info = df_plano_contas[df_plano_contas['codigo'] == conta].iloc[0]
                            saldo = valores['debito'] - valores['credito']

                            balancete_data.append({
                                'Conta': conta,
                                'Descrição': conta_info['descricao'],
                                'Tipo': conta_info.get('tipo', 'Analitico'),
                                'Débito': valores['debito'],
                                'Crédito': valores['credito'],
                                'Saldo': saldo
                            })

                    if balancete_data:
                        df_balancete = pd.DataFrame(balancete_data)
                        df_balancete = df_balancete[df_balancete['Saldo'].abs() > 0.01]
                        df_balancete.to_excel(writer, sheet_name='Balancete', index=False)

                # 2. Livro Diário
                if incluir_diario and not df_lancamentos.empty:
                    diario_data = []
                    for _, row in df_lancamentos.iterrows():
                        conta = row['conta']
                        if conta in df_plano_contas['codigo'].values:
                            conta_info = df_plano_contas[df_plano_contas['codigo'] == conta].iloc[0]

                            diario_data.append({
                                'Data': row['data'].strftime('%d/%m/%Y') if pd.notna(row['data']) else '',
                                'Conta': conta,
                                'Descrição Conta': conta_info['descricao'],
                                'Histórico': row.get('historico', ''),
                                'Débito': row.get('valor_debito', 0) if pd.notna(row.get('valor_debito', 0)) else 0,
                                'Crédito': row.get('valor_credito', 0) if pd.notna(row.get('valor_credito', 0)) else 0
                            })

                    if diario_data:
                        df_diario = pd.DataFrame(diario_data)
                        df_diario.to_excel(writer, sheet_name='Livro Diário', index=False)

                # 3. Livro Razão
                if incluir_razao and not df_lancamentos.empty:
                    razao_data = []
                    for conta in sorted(df_lancamentos['conta'].unique()):
                        if conta in df_plano_contas['codigo'].values:
                            conta_info = df_plano_contas[df_plano_contas['codigo'] == conta].iloc[0]
                            lancamentos_conta = df_lancamentos[df_lancamentos['conta'] == conta].copy()
                            lancamentos_conta = lancamentos_conta.sort_values('data')

                            saldo_acumulado = 0
                            for _, row in lancamentos_conta.iterrows():
                                debito = row.get('valor_debito', 0) if pd.notna(row.get('valor_debito', 0)) else 0
                                credito = row.get('valor_credito', 0) if pd.notna(row.get('valor_credito', 0)) else 0
                                saldo_acumulado += (debito - credito)

                                razao_data.append({
                                    'Conta': conta,
                                    'Descrição': conta_info['descricao'],
                                    'Data': row['data'].strftime('%d/%m/%Y') if pd.notna(row['data']) else '',
                                    'Histórico': row.get('historico', ''),
                                    'Débito': debito,
                                    'Crédito': credito,
                                    'Saldo': saldo_acumulado
                                })

                    if razao_data:
                        df_razao = pd.DataFrame(razao_data)
                        df_razao.to_excel(writer, sheet_name='Livro Razão', index=False)

                # 4. Balanço Patrimonial
                if incluir_balanco and not df_lancamentos.empty:
                    saldos = {}
                    for _, row in df_lancamentos.iterrows():
                        conta = row['conta']
                        valor_debito = row.get('valor_debito', 0) if pd.notna(row.get('valor_debito', 0)) else 0
                        valor_credito = row.get('valor_credito', 0) if pd.notna(row.get('valor_credito', 0)) else 0

                        if conta not in saldos:
                            saldos[conta] = 0

                        saldos[conta] += (valor_debito - valor_credito)

                    ativo = {}
                    passivo = {}
                    patrimonio_liquido = {}

                    for conta, saldo in saldos.items():
                        if abs(saldo) > 0.01 and conta in df_plano_contas['codigo'].values:
                            conta_info = df_plano_contas[df_plano_contas['codigo'] == conta].iloc[0]
                            descricao = conta_info['descricao']
                            tipo_conta = conta_info.get('tipo', 'Analitico')
                            classificacao = conta_info.get('classificacao', '')

                            codigo_str = str(conta)
                            primeiro_digito = codigo_str[0] if len(codigo_str) > 0 else ''

                            if primeiro_digito == '1':
                                ativo[conta] = {
                                    'descricao': descricao,
                                    'saldo': abs(saldo),
                                    'tipo': tipo_conta,
                                    'classificacao': classificacao
                                }
                            elif primeiro_digito == '2':
                                passivo[conta] = {
                                    'descricao': descricao,
                                    'saldo': abs(saldo),
                                    'tipo': tipo_conta,
                                    'classificacao': classificacao
                                }
                            elif primeiro_digito == '3':
                                patrimonio_liquido[conta] = {
                                    'descricao': descricao,
                                    'saldo': abs(saldo),
                                    'tipo': tipo_conta,
                                    'classificacao': classificacao
                                }

                    # Criar DataFrames do Balanço
                    balanco_data = []

                    # Ativo
                    if ativo:
                        balanco_data.append({'Grupo': 'ATIVO', 'Conta': '', 'Classificação': '', 'Descrição': '', 'Saldo': ''})
                        for codigo, info in sorted(ativo.items()):
                            balanco_data.append({
                                'Grupo': 'ATIVO',
                                'Conta': codigo,
                                'Classificação': info.get('classificacao', ''),
                                'Descrição': info['descricao'],
                                'Saldo': info['saldo']
                            })
                        balanco_data.append({'Grupo': '', 'Conta': '', 'Classificação': '', 'Descrição': 'TOTAL ATIVO', 'Saldo': sum(i['saldo'] for i in ativo.values())})
                        balanco_data.append({'Grupo': '', 'Conta': '', 'Classificação': '', 'Descrição': '', 'Saldo': ''})

                    # Passivo
                    if passivo:
                        balanco_data.append({'Grupo': 'PASSIVO', 'Conta': '', 'Classificação': '', 'Descrição': '', 'Saldo': ''})
                        for codigo, info in sorted(passivo.items()):
                            balanco_data.append({
                                'Grupo': 'PASSIVO',
                                'Conta': codigo,
                                'Classificação': info.get('classificacao', ''),
                                'Descrição': info['descricao'],
                                'Saldo': info['saldo']
                            })
                        balanco_data.append({'Grupo': '', 'Conta': '', 'Classificação': '', 'Descrição': 'TOTAL PASSIVO', 'Saldo': sum(i['saldo'] for i in passivo.values())})
                        balanco_data.append({'Grupo': '', 'Conta': '', 'Classificação': '', 'Descrição': '', 'Saldo': ''})

                    # Patrimônio Líquido
                    if patrimonio_liquido:
                        balanco_data.append({'Grupo': 'PATRIMÔNIO LÍQUIDO', 'Conta': '', 'Classificação': '', 'Descrição': '', 'Saldo': ''})
                        for codigo, info in sorted(patrimonio_liquido.items()):
                            balanco_data.append({
                                'Grupo': 'PATRIMÔNIO LÍQUIDO',
                                'Conta': codigo,
                                'Classificação': info.get('classificacao', ''),
                                'Descrição': info['descricao'],
                                'Saldo': info['saldo']
                            })
                        balanco_data.append({'Grupo': '', 'Conta': '', 'Classificação': '', 'Descrição': 'TOTAL PATRIMÔNIO LÍQUIDO', 'Saldo': sum(i['saldo'] for i in patrimonio_liquido.values())})

                    if balanco_data:
                        df_balanco = pd.DataFrame(balanco_data)
                        df_balanco.to_excel(writer, sheet_name='Balanço Patrimonial', index=False)

                # Salvar o arquivo
                writer.close()
                output.seek(0)

                # Preparar para download
                nome_arquivo = f"relatorios_contabeis_{data_inicio.strftime('%Y%m%d')}_{data_fim.strftime('%Y%m%d')}.xlsx"

                st.success("✅ Arquivo Excel gerado com sucesso!")
                st.download_button(
                    label="📥 Baixar Arquivo Excel",
                    data=output,
                    file_name=nome_arquivo,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )

            except Exception as e:
                st.error(f"Erro ao gerar arquivo Excel: {e}")
                st.exception(e)


# ==============================================================================
# FUNÇÕES DE PARCELAMENTOS
# ==============================================================================

def exibir_detalhes_parcelamento(parcelamento_id: int):
    """Exibe os detalhes completos de um parcelamento."""
    from db_manager import (
        carregar_parcelamento_por_id,
        carregar_debitos_parcelamento,
        carregar_parcelas_parcelamento,
        carregar_pagamentos_parcelamento
    )

    # Botão para voltar
    if st.button("⬅️ Voltar para Lista"):
        del st.session_state['parcelamento_selecionado']
        st.rerun()

    # Carrega dados do parcelamento
    parcelamento = carregar_parcelamento_por_id(parcelamento_id)
    if not parcelamento:
        st.warning("Parcelamento não encontrado. Pode ter sido excluído.")
        # Limpa o session_state para voltar à lista
        if 'parcelamento_selecionado' in st.session_state:
            del st.session_state['parcelamento_selecionado']
        st.rerun()
        return

    # Cabeçalho
    situacao = parcelamento.get('situacao', 'N/A')
    if situacao == 'Ativo':
        icone = "🟢"
    elif situacao == 'Rescindido':
        icone = "🔴"
    elif situacao == 'Quitado':
        icone = "✅"
    else:
        icone = "🟡"

    st.markdown(f"## {icone} Parcelamento {parcelamento.get('numero_parcelamento', 'N/A')}")

    # Tabs para organizar
    tab_info, tab_debitos, tab_parcelas, tab_pagamentos = st.tabs([
        "📋 Informações", "📊 Débitos", "📅 Parcelas", "💰 Pagamentos"
    ])

    with tab_info:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("##### Dados Básicos")
            st.write(f"**Número:** {parcelamento.get('numero_parcelamento', 'N/A')}")
            st.write(f"**CNPJ:** {parcelamento.get('cnpj', 'N/A')}")
            st.write(f"**Órgão:** {parcelamento.get('orgao', 'N/A')}")
            st.write(f"**Modalidade:** {parcelamento.get('modalidade', 'N/A')}")
            st.write(f"**Situação:** {situacao}")

        with col2:
            st.markdown("##### Datas")
            st.write(f"**Data Início:** {parcelamento.get('data_inicio', 'N/A')}")
            st.write(f"**Data Adesão:** {parcelamento.get('data_adesao', 'N/A')}")
            st.write(f"**Data Consolidação:** {parcelamento.get('data_consolidacao', 'N/A')}")
            if parcelamento.get('data_encerramento'):
                st.write(f"**Data Encerramento:** {parcelamento.get('data_encerramento')}")
            if parcelamento.get('motivo_encerramento'):
                st.write(f"**Motivo:** {parcelamento.get('motivo_encerramento')}")
            st.write(f"**Qtd. Parcelas:** {parcelamento.get('qtd_parcelas', 0)}")

        with col3:
            st.markdown("##### Valores")
            st.write(f"**Principal:** {formatar_moeda(parcelamento.get('valor_principal', 0))}")
            st.write(f"**Multa:** {formatar_moeda(parcelamento.get('valor_multa', 0))}")
            st.write(f"**Juros:** {formatar_moeda(parcelamento.get('valor_juros', 0))}")
            st.write(f"**Total Consolidado:** {formatar_moeda(parcelamento.get('valor_total_consolidado', 0))}")
            st.write(f"**Saldo Devedor:** {formatar_moeda(parcelamento.get('saldo_devedor', 0))}")

    with tab_debitos:
        df_debitos = carregar_debitos_parcelamento(parcelamento_id)
        if df_debitos.empty:
            st.info("Nenhum débito cadastrado para este parcelamento.")
        else:
            st.markdown(f"##### Lista de Débitos ({len(df_debitos)} registros)")

            # Formata valores
            colunas_exibir = []
            if 'codigo_receita' in df_debitos.columns:
                colunas_exibir.append('codigo_receita')
            if 'periodo_apuracao' in df_debitos.columns:
                colunas_exibir.append('periodo_apuracao')
            if 'valor_principal' in df_debitos.columns:
                colunas_exibir.append('valor_principal')
            if 'valor_multa' in df_debitos.columns:
                colunas_exibir.append('valor_multa')
            if 'valor_juros' in df_debitos.columns:
                colunas_exibir.append('valor_juros')
            if 'valor_total' in df_debitos.columns:
                colunas_exibir.append('valor_total')

            if colunas_exibir:
                st.dataframe(df_debitos[colunas_exibir], use_container_width=True)
            else:
                st.dataframe(df_debitos, use_container_width=True)

            # Totais
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                total_principal = df_debitos['valor_principal'].sum() if 'valor_principal' in df_debitos.columns else 0
                st.metric("Total Principal", formatar_moeda(total_principal))
            with col2:
                total_multa = df_debitos['valor_multa'].sum() if 'valor_multa' in df_debitos.columns else 0
                st.metric("Total Multa", formatar_moeda(total_multa))
            with col3:
                total_juros = df_debitos['valor_juros'].sum() if 'valor_juros' in df_debitos.columns else 0
                st.metric("Total Juros", formatar_moeda(total_juros))
            with col4:
                total_geral = df_debitos['valor_total'].sum() if 'valor_total' in df_debitos.columns else 0
                st.metric("Total Geral", formatar_moeda(total_geral))

    with tab_parcelas:
        df_parcelas = carregar_parcelas_parcelamento(parcelamento_id)
        if df_parcelas.empty:
            st.info("Nenhuma parcela cadastrada para este parcelamento.")
        else:
            st.markdown(f"##### Lista de Parcelas ({len(df_parcelas)} registros)")

            # Resumo por situação
            if 'situacao' in df_parcelas.columns:
                resumo = df_parcelas['situacao'].value_counts()
                cols = st.columns(len(resumo) + 1)
                cols[0].metric("Total", len(df_parcelas))
                for i, (sit, qtd) in enumerate(resumo.items()):
                    cols[i+1].metric(sit, qtd)

            st.markdown("---")

            # Filtro por situação
            situacoes = df_parcelas['situacao'].unique().tolist() if 'situacao' in df_parcelas.columns else []
            filtro_sit = st.multiselect("Filtrar por situação", options=situacoes, default=situacoes)

            df_parcelas_filtrado = df_parcelas
            if filtro_sit and 'situacao' in df_parcelas.columns:
                df_parcelas_filtrado = df_parcelas[df_parcelas['situacao'].isin(filtro_sit)]

            # Exibe tabela
            colunas_parcelas = ['numero_parcela', 'data_vencimento', 'valor_originario', 'saldo_atualizado', 'situacao']
            colunas_disponiveis = [c for c in colunas_parcelas if c in df_parcelas_filtrado.columns]
            st.dataframe(df_parcelas_filtrado[colunas_disponiveis], use_container_width=True, height=400)

    with tab_pagamentos:
        df_pagamentos = carregar_pagamentos_parcelamento(parcelamento_id)
        if df_pagamentos.empty:
            st.info("Nenhum pagamento registrado para este parcelamento.")
        else:
            st.markdown(f"##### Lista de Pagamentos ({len(df_pagamentos)} registros)")

            # Total pago
            total_pago = df_pagamentos['valor_pago'].sum() if 'valor_pago' in df_pagamentos.columns else 0
            st.metric("Total Pago", formatar_moeda(total_pago))

            st.markdown("---")

            # Exibe tabela
            colunas_pag = ['data_pagamento', 'valor_pago', 'darf_numero']
            colunas_disponiveis = [c for c in colunas_pag if c in df_pagamentos.columns]
            st.dataframe(df_pagamentos[colunas_disponiveis], use_container_width=True)


def exibir_formulario_edicao_parcelamento(parcelamento_id: int):
    """Exibe o formulário de edição de um parcelamento."""
    from db_manager import carregar_parcelamento_por_id, atualizar_parcelamento, carregar_plano_contas

    # Botão para voltar
    if st.button("⬅️ Voltar para Lista"):
        del st.session_state['parcelamento_editar']
        st.rerun()

    # Carrega dados do parcelamento
    parcelamento = carregar_parcelamento_por_id(parcelamento_id)
    if not parcelamento:
        st.warning("Parcelamento não encontrado. Pode ter sido excluído.")
        # Limpa o session_state para voltar à lista
        if 'parcelamento_editar' in st.session_state:
            del st.session_state['parcelamento_editar']
        st.rerun()
        return

    # Carrega plano de contas para os selectbox
    df_plano = carregar_plano_contas()
    if not df_plano.empty and 'codigo' in df_plano.columns and 'descricao' in df_plano.columns:
        opcoes_contas = [""] + [f"{row['codigo']} - {row['descricao']}" for _, row in df_plano.iterrows()]
    else:
        opcoes_contas = [""]

    def get_conta_index(conta_valor):
        """Retorna o índice da conta na lista de opções."""
        if not conta_valor:
            return 0
        for i, opcao in enumerate(opcoes_contas):
            if opcao.startswith(str(conta_valor)):
                return i
        return 0

    def extrair_codigo_conta(opcao_selecionada):
        """Extrai o código da conta da opção selecionada."""
        if not opcao_selecionada or opcao_selecionada == "":
            return None
        return opcao_selecionada.split(" - ")[0] if " - " in opcao_selecionada else opcao_selecionada

    st.markdown(f"## ✏️ Editar Parcelamento {parcelamento.get('numero_parcelamento', 'N/A')}")

    with st.form("form_editar_parcelamento"):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("##### Dados Básicos")
            numero = st.text_input("Número do Parcelamento", value=parcelamento.get('numero_parcelamento', ''))
            cnpj = st.text_input("CNPJ", value=parcelamento.get('cnpj', ''))
            orgao = st.selectbox(
                "Órgão",
                ["Receita Federal", "PGFN", "Procuradoria"],
                index=["Receita Federal", "PGFN", "Procuradoria"].index(parcelamento.get('orgao', 'Receita Federal')) if parcelamento.get('orgao') in ["Receita Federal", "PGFN", "Procuradoria"] else 0
            )
            modalidade = st.text_input("Modalidade", value=parcelamento.get('modalidade', ''))
            situacao = st.selectbox(
                "Situação",
                ["Ativo", "Rescindido", "Quitado", "Consolidado", "Suspenso"],
                index=["Ativo", "Rescindido", "Quitado", "Consolidado", "Suspenso"].index(parcelamento.get('situacao', 'Ativo')) if parcelamento.get('situacao') in ["Ativo", "Rescindido", "Quitado", "Consolidado", "Suspenso"] else 0
            )

        with col2:
            st.markdown("##### Datas")
            data_inicio = st.text_input("Data Início (YYYY-MM-DD)", value=parcelamento.get('data_inicio', '') or '')
            data_adesao = st.text_input("Data Adesão (YYYY-MM-DD)", value=parcelamento.get('data_adesao', '') or '')
            data_consolidacao = st.text_input("Data Consolidação (YYYY-MM-DD)", value=parcelamento.get('data_consolidacao', '') or '')
            data_encerramento = st.text_input("Data Encerramento (YYYY-MM-DD)", value=parcelamento.get('data_encerramento', '') or '')
            motivo_encerramento = st.text_input("Motivo Encerramento", value=parcelamento.get('motivo_encerramento', '') or '')

        st.markdown("---")
        col3, col4 = st.columns(2)

        with col3:
            st.markdown("##### Parcelas")
            qtd_parcelas = st.number_input("Qtd. Parcelas", value=int(parcelamento.get('qtd_parcelas', 0) or 0), min_value=0)
            valor_parcela = st.number_input("Valor da Parcela", value=float(parcelamento.get('valor_parcela', 0) or 0), min_value=0.0, format="%.2f")

        with col4:
            st.markdown("##### Valores")
            valor_principal = st.number_input("Valor Principal", value=float(parcelamento.get('valor_principal', 0) or 0), min_value=0.0, format="%.2f")
            valor_multa = st.number_input("Valor Multa", value=float(parcelamento.get('valor_multa', 0) or 0), min_value=0.0, format="%.2f")
            valor_juros = st.number_input("Valor Juros", value=float(parcelamento.get('valor_juros', 0) or 0), min_value=0.0, format="%.2f")
            valor_total = st.number_input("Valor Total Consolidado", value=float(parcelamento.get('valor_total_consolidado', 0) or 0), min_value=0.0, format="%.2f")
            saldo_devedor = st.number_input("Saldo Devedor", value=float(parcelamento.get('saldo_devedor', 0) or 0), min_value=0.0, format="%.2f")

        st.markdown("---")
        st.markdown("##### Contas Contábeis (para Lançamentos)")
        st.caption("Configure as contas para geração automática de lançamentos contábeis")

        col5, col6 = st.columns(2)
        with col5:
            conta_principal = st.selectbox(
                "Conta Principal (Débito Tributário)",
                options=opcoes_contas,
                index=get_conta_index(parcelamento.get('conta_contabil_principal')),
                help="Conta onde está registrado o débito tributário parcelado"
            )
            conta_multa = st.selectbox(
                "Conta Multa",
                options=opcoes_contas,
                index=get_conta_index(parcelamento.get('conta_contabil_multa')),
                help="Conta de despesa com multas (opcional)"
            )
        with col6:
            conta_juros = st.selectbox(
                "Conta Juros",
                options=opcoes_contas,
                index=get_conta_index(parcelamento.get('conta_contabil_juros')),
                help="Conta de despesa com juros (opcional)"
            )
            conta_banco = st.selectbox(
                "Conta Banco (Pagamento)",
                options=opcoes_contas,
                index=get_conta_index(parcelamento.get('conta_contabil_banco')),
                help="Conta bancária de onde saem os pagamentos"
            )

        st.markdown("---")
        st.markdown("##### Observações")
        observacoes = st.text_area("Observações", value=parcelamento.get('observacoes', '') or '', height=80)

        submitted = st.form_submit_button("💾 Salvar Alterações", type="primary", use_container_width=True)

        if submitted:
            dados_atualizacao = {
                'numero_parcelamento': numero,
                'cnpj': cnpj,
                'orgao': orgao,
                'modalidade': modalidade,
                'situacao': situacao,
                'data_inicio': data_inicio if data_inicio else None,
                'data_adesao': data_adesao if data_adesao else None,
                'data_consolidacao': data_consolidacao if data_consolidacao else None,
                'data_encerramento': data_encerramento if data_encerramento else None,
                'motivo_encerramento': motivo_encerramento if motivo_encerramento else None,
                'qtd_parcelas': qtd_parcelas,
                'valor_parcela': valor_parcela,
                'valor_principal': valor_principal,
                'valor_multa': valor_multa,
                'valor_juros': valor_juros,
                'valor_total_consolidado': valor_total,
                'saldo_devedor': saldo_devedor,
                'conta_contabil_principal': extrair_codigo_conta(conta_principal),
                'conta_contabil_multa': extrair_codigo_conta(conta_multa),
                'conta_contabil_juros': extrair_codigo_conta(conta_juros),
                'conta_contabil_banco': extrair_codigo_conta(conta_banco),
                'observacoes': observacoes if observacoes else None
            }

            if atualizar_parcelamento(parcelamento_id, dados_atualizacao):
                st.success("Parcelamento atualizado com sucesso!")
                del st.session_state['parcelamento_editar']
                st.rerun()
            else:
                st.error("Erro ao atualizar parcelamento!")


def submenu_parcelamentos_cadastro():
    """8.1 - Cadastro de Parcelamentos Tributários."""
    st.subheader("8.1 Cadastro de Parcelamentos")
    st.markdown("Gerencie os parcelamentos tributários da empresa (Receita Federal, PGFN, Procuradoria).")

    # Carregar parcelamentos existentes
    df_parcelamentos = carregar_parcelamentos()

    if df_parcelamentos.empty:
        st.info("Nenhum parcelamento cadastrado.")
        exibir_formulario_novo_parcelamento_simples()
        return

    # Criar lista de opções para selectbox
    opcoes_parcelamentos = ["-- Selecione um parcelamento --"] + [
        f"{row['id']} | {row['numero_parcelamento']} - {row.get('orgao', 'N/A')} | {row.get('situacao', 'N/A')}"
        for _, row in df_parcelamentos.iterrows()
    ]

    # Selectbox para escolher parcelamento
    parcelamento_escolhido = st.selectbox(
        "Selecione o Parcelamento:",
        opcoes_parcelamentos,
        key="selectbox_parcelamento_lista"
    )

    col_acoes = st.columns(4)
    with col_acoes[0]:
        btn_ver = st.button("🔍 Ver Detalhes", disabled=(parcelamento_escolhido == "-- Selecione um parcelamento --"))
    with col_acoes[1]:
        btn_editar = st.button("✏️ Editar", disabled=(parcelamento_escolhido == "-- Selecione um parcelamento --"))
    with col_acoes[2]:
        btn_novo = st.button("➕ Novo Parcelamento")
    with col_acoes[3]:
        btn_excluir = st.button("🗑️ Excluir", disabled=(parcelamento_escolhido == "-- Selecione um parcelamento --"))

    # Processar ações
    if btn_novo:
        st.session_state['mostrar_form_novo_parcelamento'] = True
        st.rerun()

    if st.session_state.get('mostrar_form_novo_parcelamento'):
        exibir_formulario_novo_parcelamento_simples()
        return

    if parcelamento_escolhido != "-- Selecione um parcelamento --":
        parcelamento_id = int(parcelamento_escolhido.split(" | ")[0])

        if btn_ver:
            st.session_state['parcelamento_selecionado'] = parcelamento_id
            st.rerun()

        if btn_editar:
            st.session_state['parcelamento_editar'] = parcelamento_id
            st.rerun()

        if btn_excluir:
            if excluir_parcelamento(parcelamento_id):
                st.success("Parcelamento excluído!")
                st.rerun()

    st.markdown("---")

    # Resumo
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total", len(df_parcelamentos))
    with col2:
        total_saldo = df_parcelamentos['saldo_devedor'].sum() if 'saldo_devedor' in df_parcelamentos.columns else 0
        st.metric("Saldo Devedor", formatar_moeda(total_saldo))
    with col3:
        ativos = len(df_parcelamentos[df_parcelamentos['situacao'] == 'Ativo']) if 'situacao' in df_parcelamentos.columns else 0
        st.metric("Ativos", ativos)
    with col4:
        rescindidos = len(df_parcelamentos[df_parcelamentos['situacao'] == 'Rescindido']) if 'situacao' in df_parcelamentos.columns else 0
        st.metric("Rescindidos", rescindidos)

    # Tabela simplificada
    st.markdown("### Lista de Parcelamentos")
    df_display = df_parcelamentos[['numero_parcelamento', 'orgao', 'situacao', 'saldo_devedor', 'qtd_parcelas']].copy()
    df_display.columns = ['Número', 'Órgão', 'Situação', 'Saldo Devedor', 'Parcelas']
    df_display['Saldo Devedor'] = df_display['Saldo Devedor'].apply(lambda x: formatar_moeda(x) if pd.notna(x) else 'R$ 0,00')
    st.dataframe(df_display, use_container_width=True, hide_index=True)


def exibir_formulario_novo_parcelamento_simples():
    """Formulário simplificado para novo parcelamento."""
    st.markdown("### ➕ Novo Parcelamento")

    if st.button("⬅️ Voltar"):
        if 'mostrar_form_novo_parcelamento' in st.session_state:
            del st.session_state['mostrar_form_novo_parcelamento']
        st.rerun()

    df_plano = carregar_plano_contas()
    opcoes_contas = [""] + [f"{row['codigo']} - {row['descricao']}" for _, row in df_plano.iterrows()] if not df_plano.empty else [""]

    with st.form("form_novo_parcelamento_simples"):
        col1, col2 = st.columns(2)

        with col1:
            numero = st.text_input("Número do Parcelamento *")
            cnpj = st.text_input("CNPJ")
            orgao = st.selectbox("Órgão", ["Receita Federal", "PGFN", "Procuradoria"])
            modalidade = st.text_input("Modalidade")

        with col2:
            situacao = st.selectbox("Situação", ["Ativo", "Consolidado", "Rescindido", "Quitado"])
            qtd_parcelas = st.number_input("Quantidade de Parcelas", min_value=1, value=60)
            valor_parcela = st.number_input("Valor da Parcela (R$)", min_value=0.0, format="%.2f")
            data_inicio = st.date_input("Data Início *", value=None)

        st.markdown("##### Valores")
        col1, col2, col3 = st.columns(3)
        with col1:
            valor_principal = st.number_input("Principal (R$)", min_value=0.0, format="%.2f")
        with col2:
            valor_multa = st.number_input("Multa (R$)", min_value=0.0, format="%.2f")
        with col3:
            valor_juros = st.number_input("Juros (R$)", min_value=0.0, format="%.2f")

        submitted = st.form_submit_button("💾 Salvar", use_container_width=True)

        if submitted:
            if not numero or not data_inicio:
                st.error("Número e Data Início são obrigatórios!")
            else:
                dados = {
                    'numero_parcelamento': numero,
                    'cnpj': cnpj,
                    'orgao': orgao,
                    'modalidade': modalidade,
                    'situacao': situacao,
                    'data_inicio': data_inicio.strftime('%Y-%m-%d'),
                    'qtd_parcelas': qtd_parcelas,
                    'valor_parcela': valor_parcela,
                    'valor_total_consolidado': valor_principal + valor_multa + valor_juros,
                    'valor_principal': valor_principal,
                    'valor_multa': valor_multa,
                    'valor_juros': valor_juros,
                    'saldo_devedor': valor_principal + valor_multa + valor_juros,
                }
                if salvar_parcelamento(dados):
                    st.success("Parcelamento salvo!")
                    if 'mostrar_form_novo_parcelamento' in st.session_state:
                        del st.session_state['mostrar_form_novo_parcelamento']
                    st.rerun()
                else:
                    st.error("Erro ao salvar!")


def submenu_parcelamentos_importar_pdf():
    """8.2 - Importar Arquivos do e-CAC."""
    st.subheader("8.2 Importar Arquivos do e-CAC")
    st.markdown("Importe extratos de parcelamento diretamente dos arquivos gerados pelo e-CAC da Receita Federal.")
    st.info("Você pode selecionar **múltiplos arquivos** (PDF ou XPS) de uma vez para importação em lote.")

    uploaded_files = st.file_uploader(
        "Selecione os arquivos do Extrato de Parcelamento",
        type=['pdf', 'xps'],
        accept_multiple_files=True,
        help="Faça upload de PDFs ou XPS gerados no e-CAC (Extrato de Parcelamento, PERT, etc.)"
    )

    if uploaded_files:
        import tempfile

        # Inicializa estado para armazenar resultados processados
        if 'pdfs_processados' not in st.session_state:
            st.session_state.pdfs_processados = []

        # Processa todos os arquivos
        if st.button("🔄 Processar Arquivos", type="primary"):
            st.session_state.pdfs_processados = []
            progress_bar = st.progress(0)
            status_text = st.empty()

            for i, uploaded_file in enumerate(uploaded_files):
                status_text.text(f"Processando {i+1}/{len(uploaded_files)}: {uploaded_file.name}")

                # Determina extensão do arquivo
                extensao = os.path.splitext(uploaded_file.name)[1].lower()

                # Salva arquivo temporário
                with tempfile.NamedTemporaryFile(delete=False, suffix=extensao) as tmp_file:
                    tmp_file.write(uploaded_file.getvalue())
                    tmp_path = tmp_file.name

                try:
                    resultado = parse_arquivo_parcelamento(tmp_path)
                    resultado['arquivo_nome'] = uploaded_file.name
                    resultado['tmp_path'] = tmp_path
                    st.session_state.pdfs_processados.append(resultado)
                except Exception as e:
                    st.session_state.pdfs_processados.append({
                        'arquivo_nome': uploaded_file.name,
                        'erros': [f"Erro ao processar: {str(e)}"]
                    })
                finally:
                    if os.path.exists(tmp_path):
                        os.remove(tmp_path)

                progress_bar.progress((i + 1) / len(uploaded_files))

            status_text.text(f"Processamento concluído! {len(uploaded_files)} arquivo(s) processado(s).")
            st.rerun()

        # Exibe resultados dos PDFs processados
        if st.session_state.get('pdfs_processados'):
            st.markdown("---")
            st.markdown(f"### Resultados do Processamento ({len(st.session_state.pdfs_processados)} arquivo(s))")

            # Resumo geral
            col1, col2, col3 = st.columns(3)
            total_sucesso = sum(1 for r in st.session_state.pdfs_processados if not r.get('erros'))
            total_erro = sum(1 for r in st.session_state.pdfs_processados if r.get('erros'))
            total_valor = sum(r.get('resumo_divida', {}).get('valor_total_consolidado', 0) or 0 for r in st.session_state.pdfs_processados)

            with col1:
                st.metric("Processados com Sucesso", total_sucesso)
            with col2:
                st.metric("Com Erros", total_erro)
            with col3:
                st.metric("Valor Total", formatar_moeda(total_valor))

            st.markdown("---")

            # Exibe cada PDF processado
            for idx, resultado in enumerate(st.session_state.pdfs_processados):
                arquivo_nome = resultado.get('arquivo_nome', f'Arquivo {idx+1}')

                if resultado.get('erros'):
                    with st.expander(f"❌ {arquivo_nome} - ERRO", expanded=False):
                        for erro in resultado['erros']:
                            st.error(erro)
                else:
                    dados_parc = resultado.get('dados_parcelamento', {})
                    resumo = resultado.get('resumo_divida', {})
                    parcelas = resultado.get('parcelas', [])

                    # Define ícone baseado no status
                    situacao_pdf = dados_parc.get('situacao', 'Não identificada')
                    if situacao_pdf == 'Ativo':
                        icone_pdf = "🟢"
                    elif situacao_pdf == 'Rescindido':
                        icone_pdf = "🔴"
                    elif situacao_pdf == 'Quitado':
                        icone_pdf = "✅"
                    else:
                        icone_pdf = "🟡"

                    with st.expander(f"{icone_pdf} {arquivo_nome} - {dados_parc.get('numero_parcelamento', 'N/A')} | {situacao_pdf} | {formatar_moeda(resumo.get('valor_total_consolidado', 0))}", expanded=False):
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.markdown("##### Informações Gerais")
                            st.write(f"**Número:** {dados_parc.get('numero_parcelamento', 'Não identificado')}")
                            st.write(f"**CNPJ:** {dados_parc.get('cnpj', 'Não identificado')}")
                            st.write(f"**Órgão:** {dados_parc.get('orgao', 'Não identificado')}")
                            st.write(f"**Modalidade:** {dados_parc.get('modalidade', 'Não identificada')}")
                            st.write(f"**Situação:** {situacao_pdf}")
                            st.write(f"**Qtd. Parcelas:** {dados_parc.get('qtd_parcelas') or len(parcelas)}")

                        with col2:
                            st.markdown("##### Datas")
                            st.write(f"**Data Início:** {dados_parc.get('data_inicio', 'Não identificada')}")
                            st.write(f"**Data Adesão:** {dados_parc.get('data_adesao', 'Não identificada')}")
                            st.write(f"**Data Consolidação:** {dados_parc.get('data_consolidacao', 'Não identificada')}")
                            if dados_parc.get('data_encerramento'):
                                st.write(f"**Data Encerramento:** {dados_parc.get('data_encerramento')}")
                            if dados_parc.get('motivo_encerramento'):
                                st.write(f"**Motivo:** {dados_parc.get('motivo_encerramento')}")

                        with col3:
                            st.markdown("##### Valores Consolidados")
                            st.write(f"**Total:** {formatar_moeda(resumo.get('valor_total_consolidado', 0))}")
                            st.write(f"**Principal:** {formatar_moeda(resumo.get('valor_principal', 0))}")
                            st.write(f"**Multa:** {formatar_moeda(resumo.get('valor_multa', 0))}")
                            st.write(f"**Juros:** {formatar_moeda(resumo.get('valor_juros', 0))}")
                            st.write(f"**Saldo Devedor:** {formatar_moeda(resumo.get('saldo_devedor', 0))}")

                        # Parcelas
                        if parcelas:
                            df_parcelas = pd.DataFrame(parcelas)
                            if 'situacao' in df_parcelas.columns:
                                resumo_parcelas = df_parcelas['situacao'].value_counts()
                                st.write("**Resumo Parcelas:**", resumo_parcelas.to_dict())

            # Botão para salvar todos
            st.markdown("---")
            col1, col2 = st.columns(2)
            with col1:
                if st.button("💾 Salvar TODOS os Parcelamentos", type="primary", use_container_width=True):
                    salvos = 0
                    erros = 0

                    for resultado in st.session_state.pdfs_processados:
                        if resultado.get('erros'):
                            erros += 1
                            continue

                        dados_parc = resultado.get('dados_parcelamento', {})
                        resumo = resultado.get('resumo_divida', {})
                        debitos = resultado.get('debitos', [])
                        parcelas = resultado.get('parcelas', [])

                        dados_salvar = {
                            'numero_parcelamento': dados_parc.get('numero_parcelamento') or f"IMP-{datetime.now().strftime('%Y%m%d%H%M%S')}-{salvos}",
                            'cnpj': dados_parc.get('cnpj'),
                            'orgao': dados_parc.get('orgao'),
                            'modalidade': dados_parc.get('modalidade'),
                            'situacao': dados_parc.get('situacao') or 'Ativo',
                            'data_inicio': dados_parc.get('data_inicio') or dados_parc.get('data_adesao'),
                            'data_adesao': dados_parc.get('data_adesao'),
                            'data_consolidacao': dados_parc.get('data_consolidacao'),
                            'data_encerramento': dados_parc.get('data_encerramento'),
                            'motivo_encerramento': dados_parc.get('motivo_encerramento'),
                            'qtd_parcelas': dados_parc.get('qtd_parcelas') or len(parcelas),
                            'valor_parcela': dados_parc.get('valor_parcela'),
                            'valor_total_consolidado': resumo.get('valor_total_consolidado', 0),
                            'valor_principal': resumo.get('valor_principal', 0),
                            'valor_multa': resumo.get('valor_multa', 0),
                            'valor_juros': resumo.get('valor_juros', 0),
                            'saldo_devedor': resumo.get('saldo_devedor', 0)
                        }

                        parcelamento_id = salvar_parcelamento(dados_salvar)

                        if parcelamento_id:
                            if debitos:
                                salvar_debitos_parcelamento(parcelamento_id, debitos)
                            if parcelas:
                                salvar_parcelas_parcelamento(parcelamento_id, parcelas)
                            atualizar_saldo_parcelamento(parcelamento_id)
                            salvos += 1
                        else:
                            erros += 1

                    if salvos > 0:
                        st.success(f"{salvos} parcelamento(s) importado(s) com sucesso!")
                        st.balloons()
                    if erros > 0:
                        st.warning(f"{erros} parcelamento(s) não puderam ser importados.")

                    st.session_state.pdfs_processados = []
                    st.rerun()

            with col2:
                if st.button("🗑️ Limpar Resultados", use_container_width=True):
                    st.session_state.pdfs_processados = []
                    st.rerun()


def submenu_parcelamentos_controle_parcelas():
    """8.3 - Controle de Parcelas."""
    st.subheader("8.3 Controle de Parcelas")

    df_parcelamentos = carregar_parcelamentos()

    if df_parcelamentos.empty:
        st.warning("Nenhum parcelamento cadastrado. Cadastre um parcelamento primeiro.")
        return

    # Seleção do parcelamento
    opcoes = [f"{row['numero_parcelamento']} - {row.get('orgao', 'N/A')}" for _, row in df_parcelamentos.iterrows()]
    parcelamento_selecionado = st.selectbox("Selecione o Parcelamento", opcoes)

    if parcelamento_selecionado:
        numero = parcelamento_selecionado.split(" - ")[0]
        parc_row = df_parcelamentos[df_parcelamentos['numero_parcelamento'] == numero].iloc[0]
        parcelamento_id = parc_row['id']

        # Carrega dados
        parcelamento = carregar_parcelamento_por_id(parcelamento_id)
        df_parcelas = carregar_parcelas_parcelamento(parcelamento_id)

        # Resumo
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Saldo Devedor", formatar_moeda(parcelamento.get('saldo_devedor', 0)))
        with col2:
            st.metric("Parcelas Pagas", f"{parcelamento.get('qtd_pagas', 0)}/{parcelamento.get('qtd_parcelas', 0)}")
        with col3:
            st.metric("Parcelas Vencidas", parcelamento.get('qtd_vencidas', 0))
        with col4:
            st.metric("A Vencer", parcelamento.get('qtd_a_vencer', 0))

        st.markdown("---")

        if df_parcelas.empty:
            st.info("Nenhuma parcela cadastrada para este parcelamento.")

            # Opção de gerar parcelas
            if st.button("🔄 Gerar Parcelas Automaticamente"):
                qtd = parcelamento.get('qtd_parcelas', 60)
                valor = parcelamento.get('valor_parcela', 0)
                data_inicio = datetime.strptime(parcelamento.get('data_adesao', datetime.now().strftime('%Y-%m-%d')), '%Y-%m-%d')

                parcelas_geradas = []
                for i in range(1, qtd + 1):
                    vencimento = data_inicio + pd.DateOffset(months=i-1)
                    parcelas_geradas.append({
                        'numero_parcela': i,
                        'data_vencimento': vencimento.strftime('%Y-%m-%d'),
                        'valor_originario': valor,
                        'saldo_atualizado': valor,
                        'situacao': 'A vencer'
                    })

                if salvar_parcelas_parcelamento(parcelamento_id, parcelas_geradas):
                    atualizar_saldo_parcelamento(parcelamento_id)
                    st.success(f"{qtd} parcelas geradas com sucesso!")
                    st.rerun()
        else:
            # Filtros
            col1, col2 = st.columns(2)
            with col1:
                filtro_situacao = st.multiselect("Filtrar por Situação",
                    df_parcelas['situacao'].unique().tolist() if 'situacao' in df_parcelas.columns else [],
                    default=df_parcelas['situacao'].unique().tolist() if 'situacao' in df_parcelas.columns else []
                )
            with col2:
                ordenar_por = st.selectbox("Ordenar por", ["Número", "Vencimento", "Valor", "Situação"])

            # Aplica filtros
            df_filtrado = df_parcelas.copy()
            if filtro_situacao and 'situacao' in df_filtrado.columns:
                df_filtrado = df_filtrado[df_filtrado['situacao'].isin(filtro_situacao)]

            # Ordenação
            ordem_map = {"Número": "numero_parcela", "Vencimento": "data_vencimento", "Valor": "valor_originario", "Situação": "situacao"}
            if ordem_map.get(ordenar_por) in df_filtrado.columns:
                df_filtrado = df_filtrado.sort_values(ordem_map[ordenar_por])

            # Exibe parcelas
            st.dataframe(
                df_filtrado,
                use_container_width=True,
                column_config={
                    "valor_originario": st.column_config.NumberColumn("Valor Original", format="R$ %.2f"),
                    "saldo_atualizado": st.column_config.NumberColumn("Saldo Atualizado", format="R$ %.2f"),
                    "data_vencimento": st.column_config.DateColumn("Vencimento", format="DD/MM/YYYY")
                }
            )

            # Ações em lote
            st.markdown("##### Ações")
            col1, col2, col3 = st.columns(3)
            with col1:
                if st.button("🔄 Atualizar Saldos"):
                    atualizar_saldo_parcelamento(parcelamento_id)
                    st.success("Saldos atualizados!")
                    st.rerun()


def submenu_parcelamentos_conciliacao():
    """8.4 - Conciliação de Parcelas com Extrato Bancário."""
    st.subheader("8.4 Conciliação com Extrato Bancário")

    df_parcelamentos = carregar_parcelamentos()

    if df_parcelamentos.empty:
        st.warning("Nenhum parcelamento cadastrado.")
        return

    # Seleção do parcelamento
    opcoes = [f"{row['numero_parcelamento']} - {row.get('orgao', 'N/A')}" for _, row in df_parcelamentos.iterrows()]
    parcelamento_selecionado = st.selectbox("Selecione o Parcelamento", opcoes)

    if parcelamento_selecionado:
        numero = parcelamento_selecionado.split(" - ")[0]
        parc_row = df_parcelamentos[df_parcelamentos['numero_parcelamento'] == numero].iloc[0]
        parcelamento_id = parc_row['id']

        parcelamento = carregar_parcelamento_por_id(parcelamento_id)
        df_parcelas = carregar_parcelas_parcelamento(parcelamento_id)

        if df_parcelas.empty:
            st.warning("Nenhuma parcela cadastrada para este parcelamento.")
            return

        # Parcelas não pagas
        parcelas_pendentes = df_parcelas[df_parcelas['situacao'] != 'Paga'].copy()

        st.markdown(f"**Parcelas pendentes de conciliação:** {len(parcelas_pendentes)}")

        # Configuração da conciliação
        st.markdown("##### Configurações")
        col1, col2 = st.columns(2)
        with col1:
            data_inicio = st.date_input("Data Início", value=datetime.now().date() - pd.Timedelta(days=90))
        with col2:
            data_fim = st.date_input("Data Fim", value=datetime.now().date())

        tolerancia_dias = st.slider("Tolerância de dias para vencimento", 0, 30, 5)
        tolerancia_valor = st.slider("Tolerância de valor (%)", 0.0, 5.0, 0.01)

        # Carregar contas bancárias
        df_contas = carregar_cadastro_contas()
        if df_contas.empty:
            st.warning("Nenhuma conta bancária cadastrada.")
            return

        conta_selecionada = st.selectbox(
            "Selecione a Conta Bancária",
            df_contas['Conta_OFX_Normalizada'].tolist()
        )

        if st.button("🔍 Buscar Conciliações", type="primary"):
            # Carrega extrato
            df_extrato = carregar_extrato_bancario_historico(conta_selecionada, data_inicio, data_fim)

            if df_extrato.empty:
                st.warning("Nenhuma transação encontrada no período selecionado.")
            else:
                # Filtra apenas débitos (pagamentos)
                df_debitos = df_extrato[df_extrato['Valor'] < 0].copy()

                st.write(f"**Transações de débito encontradas:** {len(df_debitos)}")

                # Executa conciliação
                conciliacoes = conciliar_parcela_extrato(
                    parcelas_pendentes,
                    df_debitos,
                    tolerancia_valor=tolerancia_valor/100,
                    tolerancia_dias=tolerancia_dias
                )

                if conciliacoes:
                    st.success(f"Encontradas {len(conciliacoes)} possíveis conciliações!")

                    df_conciliacoes = pd.DataFrame(conciliacoes)
                    st.dataframe(df_conciliacoes, use_container_width=True)

                    # Botão para confirmar conciliações
                    if st.button("✅ Confirmar Conciliações Selecionadas"):
                        for conc in conciliacoes:
                            # Atualiza parcela como paga
                            atualizar_parcela(conc['parcela_id'], {
                                'situacao': 'Paga',
                                'data_pagamento': str(conc['data_transacao']),
                                'valor_pago': conc['valor_transacao'],
                                'id_transacao_banco': conc['id_transacao']
                            })

                        atualizar_saldo_parcelamento(parcelamento_id)
                        st.success("Conciliações confirmadas!")
                        st.rerun()
                else:
                    st.info("Nenhuma conciliação automática encontrada. Verifique os parâmetros ou concilie manualmente.")


def submenu_parcelamentos_lancamentos():
    """8.5 - Geração de Lançamentos Contábeis para Parcelamentos."""
    st.subheader("8.5 Lançamentos Contábeis de Parcelamentos")
    st.markdown("Gere os lançamentos contábeis separando **Principal**, **Multa** e **Juros**.")

    df_parcelamentos = carregar_parcelamentos()

    if df_parcelamentos.empty:
        st.warning("Nenhum parcelamento cadastrado.")
        return

    # Seleção do parcelamento
    opcoes = [f"{row['numero_parcelamento']} - {row.get('orgao', 'N/A')}" for _, row in df_parcelamentos.iterrows()]
    parcelamento_selecionado = st.selectbox("Selecione o Parcelamento", opcoes)

    if parcelamento_selecionado:
        numero = parcelamento_selecionado.split(" - ")[0]
        parc_row = df_parcelamentos[df_parcelamentos['numero_parcelamento'] == numero].iloc[0]
        parcelamento_id = parc_row['id']

        parcelamento = carregar_parcelamento_por_id(parcelamento_id)
        df_parcelas = carregar_parcelas_parcelamento(parcelamento_id)

        # Verifica se tem contas contábeis configuradas
        if not parcelamento.get('conta_contabil_principal') or not parcelamento.get('conta_contabil_banco'):
            st.warning("⚠️ Este parcelamento não possui contas contábeis configuradas. Configure no cadastro do parcelamento.")
            return

        # Exibe proporções
        valor_total = (parcelamento.get('valor_principal', 0) or 0) + \
                      (parcelamento.get('valor_multa', 0) or 0) + \
                      (parcelamento.get('valor_juros', 0) or 0)

        if valor_total > 0:
            prop_principal = (parcelamento.get('valor_principal', 0) or 0) / valor_total * 100
            prop_multa = (parcelamento.get('valor_multa', 0) or 0) / valor_total * 100
            prop_juros = (parcelamento.get('valor_juros', 0) or 0) / valor_total * 100

            st.markdown("##### Proporção para Rateio")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Principal", f"{prop_principal:.2f}%")
            with col2:
                st.metric("Multa", f"{prop_multa:.2f}%")
            with col3:
                st.metric("Juros", f"{prop_juros:.2f}%")

        st.markdown("---")

        # Tabs
        tab_manual, tab_parcelas = st.tabs(["📝 Lançamento Manual", "📋 Por Parcelas Pagas"])

        with tab_manual:
            st.markdown("##### Gerar Lançamento Manual")

            col1, col2 = st.columns(2)
            with col1:
                data_pagamento = st.date_input("Data do Pagamento", value=datetime.now().date())
            with col2:
                valor_pago = st.number_input("Valor Pago (R$)", min_value=0.01, format="%.2f")

            if st.button("📄 Gerar Lançamentos", type="primary"):
                if valor_pago > 0:
                    lancamentos = gerar_lancamentos_parcelamento(
                        parcelamento,
                        valor_pago,
                        data_pagamento.strftime('%Y-%m-%d')
                    )

                    if lancamentos:
                        st.markdown("##### Preview dos Lançamentos")

                        for lanc in lancamentos:
                            st.write(f"""
                            **{lanc['historico']}**
                            - Data: {lanc['data_lancamento']}
                            - D: {lanc['reduz_deb']} ({lanc['nome_conta_d']})
                            - C: {lanc['reduz_cred']} ({lanc['nome_conta_c']})
                            - Valor: {formatar_moeda(lanc['valor'])}
                            """)

                        if st.button("💾 Salvar Lançamentos"):
                            for lanc in lancamentos:
                                salvar_partidas_lancamento([lanc])
                            st.success(f"{len(lancamentos)} lançamentos salvos com sucesso!")

        with tab_parcelas:
            st.markdown("##### Gerar Lançamentos para Parcelas Pagas")

            # Filtra parcelas pagas que não tem lançamento gerado
            parcelas_pagas = df_parcelas[df_parcelas['situacao'] == 'Paga'].copy() if not df_parcelas.empty else pd.DataFrame()

            if parcelas_pagas.empty:
                st.info("Nenhuma parcela paga encontrada.")
            else:
                st.dataframe(parcelas_pagas, use_container_width=True)

                if st.button("📄 Gerar Lançamentos para Todas"):
                    total_lancamentos = 0
                    for _, parcela in parcelas_pagas.iterrows():
                        valor = parcela.get('valor_pago') or parcela.get('valor_originario', 0)
                        data = parcela.get('data_pagamento') or parcela.get('data_vencimento')

                        if valor and data:
                            lancamentos = gerar_lancamentos_parcelamento(
                                parcelamento,
                                valor,
                                str(data)
                            )
                            for lanc in lancamentos:
                                salvar_partidas_lancamento([lanc])
                                total_lancamentos += 1

                    st.success(f"{total_lancamentos} lançamentos gerados com sucesso!")


if __name__ == "__main__":
    main()