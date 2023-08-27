# importar funões
from funcoes import *

# Definir variáveis
staging_area = "staging"
data_folder = "data"
database = "base_estban.csv"
update_file = "update_control.csv"

print('-----------------------------------------------------------------')
print(f'Atualização da base {database}')
print('-----------------------------------------------------------------')

##########################################################################
#                               Executar ETL                             #
##########################################################################
while True:
    # Verificar data de atualização
    data_update = update_date(data_folder,update_file)
    # Extrair dados da fonte
    if not extract(staging_area,data_update):
        break

    # Ralizar as transformações nos dados
    df_load = transform(staging_area,data_update)

    # Salvar os dados no arquivo final
    load(data_folder,database,df_load)

    # Atualiza tabela de controle de atualização
    update_control(data_folder,update_file,data_update)

    # Deletar arquivo carregado
    delete_file(staging_area,data_update)

print('-----------------------------------------------------------------')
print("Fim da execução do programa!")
print('-----------------------------------------------------------------')

