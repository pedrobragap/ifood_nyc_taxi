# NYC Taxi Trip Records Pipeline

Este projeto implementa um pipeline de dados para processar registros de corridas de táxi de Nova York, utilizando Databricks Community Edition e Delta Lake. O pipeline é dividido em duas camadas: Bronze e Silver.

## Estrutura do Projeto

```
ifood_nyc_taxi/
├── src/
│   ├── bronze.ipynb    # Camada de ingestão e transformação inicial
│   ├── silver.ipynb    # Camada de refinamento e qualidade
│   ├── test_pipeline.py # Testes automatizados
│   └── elt_nyc_taxi.yaml # Configurações do pipeline
└── requirements.txt    # Dependências do projeto
```

## Pré-requisitos

- Conta no Databricks Community Edition (https://community.cloud.databricks.com/)
- Acesso à internet para download dos dados

## Configuração no Databricks Community Edition

1. **Acessar o Databricks Community Edition**:
   - Acesse https://community.cloud.databricks.com/
   - Faça login ou crie uma conta gratuita

2. **Criar um novo Workspace**:
   - Após o login, você terá acesso a um workspace gratuito
   - O Databricks Community Edition já vem com um cluster pré-configurado
   - Não é necessário criar ou configurar clusters adicionais

3. **Importar o Projeto**:
   - No menu lateral, clique em "Workspace"
   - Clique em "New" > "More" > "Git Folder"
   - Na janela de importação, cole a URL do repositório:
     ```
     https://github.com/pedrobragap/ifood_nyc_taxi/
     ```
   - Clique em "Create"
   - O projeto será clonado automaticamente no seu workspace

## Configuração do Pipeline

O projeto usa um arquivo YAML (`elt_nyc_taxi.yaml`) para configuração. Você pode ajustar os seguintes parâmetros:

```yaml
# Configurações do Databricks
databricks:
  workspace: community
  landing_path: "/dbfs/FileStore/nyc_taxi_trip_records"

# Configurações das Camadas
layers:
  bronze:
    schema: bronze
    table: nyc_taxi_trip_records
    partition_by: ["year", "month"]
    
  silver:
    schema: silver
    table: nyc_taxi_trip_records
    partition_by: ["year", "month"]
    filters:
      year: 2023
      start_month: 1
      end_month: 5
      min_amount: 0
      min_passengers: 0
    columns:
      - VendorID
      - passenger_count
      - total_amount
      - tpep_pickup_datetime
      - tpep_dropoff_datetime
      - taxi_color
      - year
      - month

# Configurações dos Dados
data:
  source_url: "https://d37ci6vzurychx.cloudfront.net/trip-data"
  taxi_colors: ["yellow", "green"]
  year: 2023
  months: [1, 2, 3, 4, 5]
```

## Como Usar

### 1. Camada Bronze

O notebook `bronze.ipynb` é responsável por:
- Baixar os dados de táxi amarelo e verde de NYC
- Aplicar transformações iniciais
- Armazenar os dados em formato Delta Lake

Para executar:
1. Abra o notebook `bronze.ipynb`
2. As configurações são carregadas automaticamente do arquivo YAML
3. Execute todas as células

### 2. Camada Silver

O notebook `silver.ipynb` é responsável por:
- Ler dados da camada bronze
- Aplicar regras de qualidade
- Remover duplicatas
- Criar uma versão refinada dos dados

Para executar:
1. Abra o notebook `silver.ipynb`
2. As configurações são carregadas automaticamente do arquivo YAML
3. Execute todas as células

### 3. Executando os Testes

Para executar os testes automatizados:
1. Abra um novo notebook
2. Execute:
   ```python
   from test_pipeline import TestTaxiPipeline
   import unittest
   
   suite = unittest.TestLoader().loadTestsFromTestCase(TestTaxiPipeline)
   unittest.TextTestRunner(verbosity=2).run(suite)
   ```

## Limitações do Community Edition

1. **Recursos**:
   - Cluster único com recursos limitados
   - Tempo de execução limitado (2 horas por sessão)
   - Armazenamento limitado

2. **Funcionalidades**:
   - Sem suporte a Volumes (use DBFS)
   - Sem suporte a Jobs
   - Sem suporte a Repos
   - Sem suporte a MLflow

3. **Recomendações**:
   - Reduza o volume de dados para testes
   - Use particionamento para melhor performance
   - Faça backup dos dados importantes

## Estrutura dos Dados

### Camada Bronze
- Dados brutos com todas as colunas originais
- Particionado por ano e mês
- Mantém o histórico completo

### Camada Silver
- Dados refinados com colunas selecionadas
- Filtros de qualidade aplicados:
  - `total_amount > 0`
  - `passenger_count > 0`
- Sem duplicatas
- Particionado por ano e mês

## Colunas Principais

- `VendorID`: ID do fornecedor
- `passenger_count`: Número de passageiros
- `tpep_pickup_datetime`: Data/hora de início da corrida
- `tpep_dropoff_datetime`: Data/hora de fim da corrida
- `total_amount`: Valor total da corrida
- `taxi_color`: Cor do táxi (yellow/green)
- `year`: Ano da corrida
- `month`: Mês da corrida

## Monitoramento

Para monitorar o pipeline:
1. Verifique as tabelas no catálogo do Databricks
2. Use o comando `DESCRIBE HISTORY` para ver o histórico de alterações:
   ```sql
   DESCRIBE HISTORY bronze.nyc_taxi_trip_records;
   DESCRIBE HISTORY silver.nyc_taxi_trip_records;
   ```

## Troubleshooting

1. **Erro de timeout**:
   - Reduza o volume de dados
   - Divida o processamento em partes menores
   - Reinicie a sessão se necessário

2. **Erro de memória**:
   - Reduza o tamanho dos lotes de processamento
   - Use particionamento mais granular
   - Limpe dados temporários

3. **Erro de conexão**:
   - Verifique a conexão com a internet
   - Verifique se as URLs dos dados estão acessíveis
   - Reinicie a sessão se necessário

## Contribuindo

1. Faça um fork do projeto
2. Crie uma branch para sua feature (`git checkout -b feature/nova-feature`)
3. Commit suas mudanças (`git commit -am 'Adiciona nova feature'`)
4. Push para a branch (`git push origin feature/nova-feature`)
5. Crie um Pull Request