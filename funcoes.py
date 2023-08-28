import os
from urllib import request
from pyunpack import Archive
import pandas as pd

def update_date(data_folder,update_file):
    print('Definindo a data de atualização...')
    if os.path.exists(f'{data_folder}/{update_file}'):
        df_update = pd.read_csv(os.path.join(data_folder, update_file), delimiter=',')
        update_control = df_update['update_control'][0]
        ano = str(update_control)[:4]
        mes = str(update_control)[4:]
        if int(mes) == 12:
            next_month = 1
            next_year = int(ano) + 1
        else:
            next_month = int(mes) + 1
            next_year = int(ano)
        data_update = str(next_year) + str(next_month).zfill(2)
    else:
        data_update = '198807'
    print(f'Serão carregados os dados de {data_update}.\n')
    return data_update

def extract(staging_area,data_update):
    # Verifica se existe o diretório para os dados, se não existe, criar
    if not os.path.exists(staging_area):
        os.makedirs(staging_area)

    print('-----------------------------------------------------------------')
    print('Extração de arquivos')
    print('-----------------------------------------------------------------')

    # Definir link e arquivo
    file = f'{data_update}_ESTBAN.ZIP'
    link = f'https://www4.bcb.gov.br/fis/cosif/cont/estban/municipio/{file}'

    # Verifica se o arquivo já foi baixado
    if os.path.exists(f'{staging_area}/{file}'):
        print(f'Arquivo {file} já foi baixado.')
    else:
    # Tenta baixar o arquivo
        try:
            print(f'Baixando o arquivo {file}...')
            request.urlretrieve(f'{link}', f'{staging_area}/{file}')
        except:
            print(f'Arquivo de {data_update} ainda não disponibilizado pela fonte.')
            print('Todos os arquivos disponíveis foram baixados!')
            print('-----------------------------------------------------------------')
            return False

        # Verifica se o arquivo foi baixado
        if os.path.exists(f'{staging_area}/{file}'):
            print(f'Arquivo {file} baixado.')
            # Descompacta o arquivo
            print(f'Descompactando o arquivo {file}...')
            Archive(f'{staging_area}/{file}').extractall(f'{staging_area}')
            print(f'Arquivo {file} descompactando.')
            print('-----------------------------------------------------------------')
            # Remove o arquivo compactado
            try:
                os.remove(f'{staging_area}/{file}')
            except OSError as e:
                print(f"Error:{ e.strerror}")
        else:
            print(f'Não foi possível baixar o arquivo {file}.')
            print('-----------------------------------------------------------------')
            return False
        
        return True

def transform(staging_area,data_update):
    print('-----------------------------------------------------------------')
    print('Transformação de dados')
    print('-----------------------------------------------------------------')
    print('Realizando as transformações necessárias nos dados...')

    # Definir o arquivo que será lido
    filename = f'{data_update}_ESTBAN.CSV'
    # Ler o arquivo
    df = pd.read_csv(os.path.join(staging_area, filename), delimiter=';', encoding='ISO-8859-1', skiprows=2)
    # Filtrar o estado
    df = df[df['UF'] == 'SC']
    # Filtrar os municípios
    filtro_municipios = ['BLUMENAU', 'CHAPECO', 'CRICIUMA', 'ITAJAI', 'FLORIANOPOLIS']
    df = df[df['MUNICIPIO'].isin(filtro_municipios)]
    # Renomear colunas
    df = df.rename(columns={'#DATA_BASE': 'DATA_BASE'})
    # Selecionar as colunas desejadas
    df = df[['DATA_BASE','UF','MUNICIPIO','NOME_INSTITUICAO','VERBETE_160_OPERACOES_DE_CREDITO','VERBETE_420_DEPOSITOS_DE_POUPANCA','VERBETE_432_DEPOSITOS_A_PRAZO']]
    print('Dataframe criado com sucesso para a carga dos dados.')
    return df

def load(data_folder,database,df_load):
    print('-----------------------------------------------------------------')
    print('Carga dos dados')
    print('-----------------------------------------------------------------')

    # Verificar se existe o diretório para os dados, se não existir, criar
    if not os.path.exists(data_folder):
        os.makedirs(data_folder)

    print(f'Carregando os dados no arquivo {database}...')
    # Verificar se a base de dados já existe
    if os.path.exists(f'{data_folder}/{database}'):
        # Se o arquivo já existe, adicionar dados sem o cabeçalho
        df_load.to_csv(os.path.join(data_folder, database), mode='a', header=False, index=False)
    else:
        # Se o arquivo não existe, criar e salvar com cabeçalho
        df_load.to_csv(os.path.join(data_folder, database), index=False)
    print(f'Dados carregados com sucesso no arquivo {database}.')

def update_control(data_folder,update_file,data_update):
    print('-----------------------------------------------------------------')
    print(f'Atualizando a tabela {update_file}...')
    df = pd.DataFrame({'update_control': [data_update]})
    df.to_csv(os.path.join(data_folder, update_file), index=False)
    print(f'Tabela {update_file} atualizada.')
    print('-----------------------------------------------------------------')

def delete_file(staging_area,data_update):
    # Define o arquivo a ser excluído
    filename = f'{data_update}_ESTBAN.CSV'
    # Remove o arquivo já carregado
    try:
        os.remove(f'{staging_area}/{filename}')
    except OSError as e:
        print(f"Error:{ e.strerror}")

