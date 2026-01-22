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

# Engine SQLAlchemy (para pandas.to_sql)
_sqlalchemy_engine = None


def get_sqlalchemy_engine():
    """Retorna uma engine SQLAlchemy para uso com pandas.to_sql()."""
    global _sqlalchemy_engine
    if _sqlalchemy_engine is None:
        from sqlalchemy import create_engine
        if IS_PRODUCTION:
            url = DATABASE_URL
            if url.startswith('postgres://'):
                url = url.replace('postgres://', 'postgresql://', 1)
            _sqlalchemy_engine = create_engine(url)
        else:
            _sqlalchemy_engine = create_engine(f'sqlite:///{SQLITE_FILE}')
    return _sqlalchemy_engine


def get_placeholder():
    """Retorna o placeholder correto para o banco atual."""
    return '%s' if IS_PRODUCTION else '?'


def get_db_type():
    """Retorna o tipo do banco de dados atual."""
    return 'postgresql' if IS_PRODUCTION else 'sqlite'


def adapt_query(query):
    """Adapta uma query para o banco de dados atual (converte ? para %s se PostgreSQL)."""
    if IS_PRODUCTION:
        return query.replace('?', '%s')
    return query


class AdaptedCursor:
    """Wrapper de cursor que adapta automaticamente queries para PostgreSQL."""
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, query, params=None):
        adapted = adapt_query(query)
        if params is not None:
            return self._cursor.execute(adapted, params)
        return self._cursor.execute(adapted)

    def executemany(self, query, params_list):
        adapted = adapt_query(query)
        return self._cursor.executemany(adapted, params_list)

    def fetchone(self):
        return self._cursor.fetchone()

    def fetchall(self):
        return self._cursor.fetchall()

    @property
    def lastrowid(self):
        return self._cursor.lastrowid

    @property
    def rowcount(self):
        return self._cursor.rowcount

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class AdaptedConnection:
    """Wrapper de conexão que retorna cursores adaptados."""
    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return AdaptedCursor(self._conn.cursor())

    def commit(self):
        return self._conn.commit()

    def rollback(self):
        return self._conn.rollback()

    def close(self):
        return self._conn.close()

    def execute(self, query, params=None):
        """Executa diretamente na conexão (para pandas.to_sql e outros)."""
        adapted = adapt_query(query)
        cursor = self._conn.cursor()
        if params:
            cursor.execute(adapted, params)
        else:
            cursor.execute(adapted)
        return cursor

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_connection():
    """Retorna uma conexão com o banco de dados apropriado."""
    if IS_PRODUCTION:
        # PostgreSQL na nuvem
        url = DATABASE_URL
        if url.startswith('postgres://'):
            url = url.replace('postgres://', 'postgresql://', 1)
        return psycopg2.connect(url)
    else:
        # SQLite local
        return sqlite3.connect(SQLITE_FILE)


@contextmanager
def get_db_connection():
    """Context manager para conexão com o banco (com adaptação automática de queries)."""
    conn = get_connection()
    adapted = AdaptedConnection(conn)
    try:
        yield adapted
    finally:
        conn.close()


def execute_query(query, params=None, fetch=False, fetchone=False, commit=True):
    """
    Executa uma query no banco de dados.
    Adapta automaticamente a sintaxe para SQLite ou PostgreSQL.
    """
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


# Informações para debug
if __name__ == '__main__':
    print(f"Ambiente: {'PRODUCAO (PostgreSQL)' if IS_PRODUCTION else 'LOCAL (SQLite)'}")
    print(f"DATABASE_URL: {'Configurado' if DATABASE_URL else 'Nao configurado'}")
