"""
Módulo de conexão com banco de dados.
Suporta SQLite (local) e PostgreSQL (produção/nuvem).
"""
import os
from contextlib import contextmanager

# Tenta carregar variáveis de ambiente do .env (desenvolvimento local)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Detecta se está em produção (PostgreSQL) ou local (SQLite)
DATABASE_URL = os.environ.get('DATABASE_URL')
IS_PRODUCTION = DATABASE_URL is not None

if IS_PRODUCTION:
    import psycopg2
    from psycopg2.extras import RealDictCursor
else:
    import sqlite3

# Nome do arquivo SQLite para desenvolvimento local
SQLITE_FILE = 'conciliacao_db.sqlite'


def get_connection():
    """Retorna uma conexão com o banco de dados apropriado."""
    if IS_PRODUCTION:
        # PostgreSQL na nuvem
        # Railway usa DATABASE_URL no formato: postgresql://user:pass@host:port/db
        url = DATABASE_URL
        if url.startswith('postgres://'):
            url = url.replace('postgres://', 'postgresql://', 1)
        return psycopg2.connect(url)
    else:
        # SQLite local
        return sqlite3.connect(SQLITE_FILE)


@contextmanager
def get_db_connection():
    """Context manager para conexão com o banco."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def execute_query(query, params=None, fetch=False, fetchone=False, commit=True):
    """
    Executa uma query no banco de dados.
    Adapta automaticamente a sintaxe para SQLite ou PostgreSQL.
    """
    # Adaptar placeholders: SQLite usa ?, PostgreSQL usa %s
    if IS_PRODUCTION and params:
        query = query.replace('?', '%s')

    with get_db_connection() as conn:
        cursor = conn.cursor()

        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)

        result = None
        if fetchone:
            result = cursor.fetchone()
        elif fetch:
            result = cursor.fetchall()

        if commit:
            conn.commit()

        return result


def adapt_schema_for_postgres(schema_sql):
    """
    Adapta SQL de criação de schema do SQLite para PostgreSQL.
    """
    if not IS_PRODUCTION:
        return schema_sql

    # Substituições necessárias
    sql = schema_sql
    sql = sql.replace('AUTOINCREMENT', '')  # PostgreSQL usa SERIAL
    sql = sql.replace('INTEGER PRIMARY KEY', 'SERIAL PRIMARY KEY')

    return sql


def table_exists(table_name):
    """Verifica se uma tabela existe no banco."""
    if IS_PRODUCTION:
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s
            )
        """
        result = execute_query(query, (table_name,), fetchone=True)
        return result[0] if result else False
    else:
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        result = execute_query(query, (table_name,), fetchone=True, commit=False)
        return result is not None


def get_placeholder():
    """Retorna o placeholder correto para o banco atual."""
    return '%s' if IS_PRODUCTION else '?'


def get_db_type():
    """Retorna o tipo do banco de dados atual."""
    return 'postgresql' if IS_PRODUCTION else 'sqlite'


# Informações para debug
if __name__ == '__main__':
    print(f"Ambiente: {'PRODUÇÃO (PostgreSQL)' if IS_PRODUCTION else 'LOCAL (SQLite)'}")
    print(f"DATABASE_URL: {'Configurado' if DATABASE_URL else 'Não configurado'}")
