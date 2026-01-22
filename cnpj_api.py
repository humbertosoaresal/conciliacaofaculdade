import requests
import re
import streamlit as st

def limpar_cnpj(cnpj: str) -> str:
    """Remove caracteres não numéricos do CNPJ."""
    return re.sub(r'\D', '', cnpj)

def validar_cnpj(cnpj: str) -> bool:
    """Valida se o CNPJ tem 14 dígitos."""
    cnpj_limpo = limpar_cnpj(cnpj)
    return len(cnpj_limpo) == 14

def formatar_cnpj(cnpj: str) -> str:
    """Formata o CNPJ no padrão XX.XXX.XXX/XXXX-XX."""
    cnpj_limpo = limpar_cnpj(cnpj)
    if len(cnpj_limpo) != 14:
        return cnpj
    return f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:]}"

def buscar_cnpj_api(cnpj: str) -> dict:
    """
    Busca os dados de um CNPJ utilizando a API gratuita da ReceitaWS.
    API: https://receitaws.com.br/

    Retorna um dicionário com os dados da empresa ou None em caso de erro.
    """
    cnpj_limpo = limpar_cnpj(cnpj)

    if not validar_cnpj(cnpj_limpo):
        st.error("CNPJ inválido! O CNPJ deve ter 14 dígitos.")
        return None

    url = f"https://www.receitaws.com.br/v1/cnpj/{cnpj_limpo}"

    try:
        with st.spinner(f"Buscando dados do CNPJ {formatar_cnpj(cnpj_limpo)}..."):
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                dados = response.json()

                # Verifica se houve erro na API
                if dados.get('status') == 'ERROR':
                    st.error(f"Erro da API: {dados.get('message', 'CNPJ não encontrado')}")
                    return None

                # Formatar atividade principal
                atividade_principal = ''
                if dados.get('atividade_principal'):
                    atividade_princ = dados['atividade_principal'][0] if isinstance(dados['atividade_principal'], list) else dados['atividade_principal']
                    if isinstance(atividade_princ, dict):
                        codigo = atividade_princ.get('code', '')
                        texto = atividade_princ.get('text', '')
                        atividade_principal = f"{codigo} - {texto}" if codigo and texto else texto or codigo
                    else:
                        atividade_principal = str(atividade_princ)

                # Formatar atividades secundárias
                atividades_secundarias = ''
                if dados.get('atividades_secundarias'):
                    ativ_sec_list = []
                    for ativ in dados['atividades_secundarias']:
                        if isinstance(ativ, dict):
                            codigo = ativ.get('code', '')
                            texto = ativ.get('text', '')
                            ativ_formatada = f"{codigo} - {texto}" if codigo and texto else texto or codigo
                            ativ_sec_list.append(ativ_formatada)
                        else:
                            ativ_sec_list.append(str(ativ))
                    atividades_secundarias = ' | '.join(ativ_sec_list)

                # Mapeia os dados da API para o formato do banco de dados
                empresa_dados = {
                    'cnpj': cnpj_limpo,
                    'razao_social': dados.get('nome', ''),
                    'nome_fantasia': dados.get('fantasia', ''),
                    'inscricao_estadual': '',  # API não retorna
                    'inscricao_municipal': '',  # API não retorna
                    'logradouro': dados.get('logradouro', ''),
                    'numero': dados.get('numero', ''),
                    'complemento': dados.get('complemento', ''),
                    'bairro': dados.get('bairro', ''),
                    'municipio': dados.get('municipio', ''),
                    'uf': dados.get('uf', ''),
                    'cep': dados.get('cep', ''),
                    'telefone': dados.get('telefone', ''),
                    'email': dados.get('email', ''),
                    'data_abertura': dados.get('abertura', ''),
                    'situacao': dados.get('situacao', ''),
                    'atividade_principal': atividade_principal,
                    'atividades_secundarias': atividades_secundarias,
                }

                st.success(f"Dados da empresa '{empresa_dados['razao_social']}' encontrados com sucesso!")
                return empresa_dados

            elif response.status_code == 429:
                st.error("Limite de requisições excedido. Aguarde alguns minutos e tente novamente.")
                return None
            else:
                st.error(f"Erro ao buscar CNPJ: Status {response.status_code}")
                return None

    except requests.exceptions.Timeout:
        st.error("Tempo de espera excedido. Verifique sua conexão com a internet.")
        return None
    except requests.exceptions.ConnectionError:
        st.error("Erro de conexão. Verifique sua internet.")
        return None
    except Exception as e:
        st.error(f"Erro inesperado ao buscar CNPJ: {e}")
        return None
