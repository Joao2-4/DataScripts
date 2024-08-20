# Importação de bibliotecas
import pandas as pd
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from urllib.parse import quote
from datetime import datetime  # Importa para obter a data atual
from tqdm import tqdm  # Importa tqdm para a barra de progresso

# Variáveis para acessar a conexão ao Banco
DB_SCHEME = 'mssql+pymssql'
DB_HOST = r'xxxxxxxxx' # Banco
DB_DATABASE = 'DWCORPORATIVO' 
DB_USERNAME = 'xxxxxxxxxxx' # Usuario
DB_PASSWORD = quote('xxxxxxxxxxxx') # Senha
db_user_data = f'{DB_USERNAME}:{DB_PASSWORD}@' if DB_USERNAME and DB_PASSWORD else ''
DB_URI = fr'{DB_SCHEME}://{db_user_data}{DB_HOST}/{DB_DATABASE}?charset=utf8'

# Criação do mecanismo SQLAlchemy
database_engine = create_engine(DB_URI)
DatabaseSession = scoped_session(sessionmaker(bind=database_engine))

# Função para ler o Excel
def ler_excel_para_df(caminho):
    dados_df = pd.read_excel(caminho)
    return dados_df

# Função para inserir dados na tabela DimResultadoIDEB_Homolog
def inserir_dados(session, dados_df):
    metadata = MetaData()
    metadata.reflect(bind=database_engine)
    tabela = Table('DimResultadoIDEB_Homolog', metadata, autoload_with=database_engine, schema='dbo')

    dados_inseridos = 0
    data_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Cria a barra de progresso com tqdm
    with session.connection() as connection:  # Usa a sessão corretamente
        for _, row in tqdm(dados_df.iterrows(), total=dados_df.shape[0], desc='Inserindo registros'):
            dados = row.to_dict()
            id_resultado = dados.pop('IdResultadoIDEBH', None)
            
            # Substitui NaN por None
            dados = {k: (None if pd.isna(v) else v) for k, v in dados.items()}
            dados['DataCarga'] = data_atual

            if id_resultado:
                # Verifica se o registro já existe
                stmt = tabela.select().where(tabela.c.IdResultadoIDEBH == id_resultado)
                result = connection.execute(stmt).fetchone()

                if not result:
                    # Insere um novo registro
                    stmt = tabela.insert().values(**dados, IdResultadoIDEBH=id_resultado)
                    result = connection.execute(stmt)
                    if result.rowcount > 0:
                        dados_inseridos += result.rowcount

        session.flush()  # Força a aplicação das alterações
        session.commit()  # Confirma as alterações no banco de dados

    return dados_inseridos

# Execução do processo
caminho_excel = 'Dados_Atualizados.xlsx'
dados_df = ler_excel_para_df(caminho_excel)

# Usando a sessão corretamente
session = DatabaseSession()
total_inseridos = inserir_dados(session, dados_df)

print(f'Total de registros inseridos: {total_inseridos}')

# Fecha a sessão ao final
session.close()
