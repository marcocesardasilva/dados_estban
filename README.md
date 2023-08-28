# Pipeline de Dados do ESTBAN

## ETSBAN: Estatística Bancária Mensal por município

Pipeline desenvolvido em Python para baixar todos os arquivos históricos do ESTBAN desde julho de 1988 até a última disponibilização pela fonte, e salvar em um arquivo CSV único, com filtros pré-definidos para o estado de SC e os seguintes municípios: Blumenau, Chapecó, Criciúma, Itajaí e Florianópolis.

A atualização é incremental, podendo ser colocada em algum agendador de tarefas para rodar periodicamente e ter sempre os dados atualizados.
