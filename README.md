# NYC Taxi Trip Records Pipeline

Este projeto implementa um pipeline de dados para processar registros de corridas de táxi de Nova York, utilizando Databricks Community Edition e Delta Lake. O pipeline é dividido em duas camadas: Bronze e Silver.

## Estrutura do Projeto

```
ifood_nyc_taxi/
├── analysis/
│   ├── perguntas/
│   │    ├──gold.ipynb
│   ├──EDA.ipynb
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

4. **Configurar o Job**:
   - No menu lateral, clique em "Workflows"
   - Clique em "Create Job"
   - Clique nos três pontos (...) no canto superior direito
   - Selecione "Edit YAML"
   - Cole o seguinte YAML:
   - deve subistituir o email_usuario para o email que criou a conta
     ```resources:
        jobs:
            New_Job_Jun_14_2025_06_35_PM:
            name: New Job Jun 14, 2025, 06:35 PM
            tasks:
                - task_key: bronze
                notebook_task:
                    notebook_path: /Workspace/Users/{email_usuario}/ifood_nyc_taxi/src/bronze
                    source: WORKSPACE
                - task_key: silver
                depends_on:
                    - task_key: bronze
                notebook_task:
                    notebook_path: /Workspace/Users/{email_usuario}/ifood_nyc_taxi/src/silver
                    source: WORKSPACE
            queue:
                enabled: true
            performance_target: STANDARD```

## Como Usar

### 1. Executando o Workflow

Para executar o pipeline de dados:

1. No menu lateral, clique em "Workflows"
2. Localize o job "etl_nyc_taxi"
3. Clique no botão "Run Now" (▶️)
4. Aguarde a execução completa
   - O job executará primeiro a camada Bronze
   - Em seguida, executará a camada Silver
   - Você pode acompanhar o progresso em tempo real

### 2. Monitorando a Execução

Durante a execução, você pode:
- Ver o status de cada task (Bronze e Silver)
- Acessar os logs de execução
- Verificar os dados processados nas tabelas:
  - Bronze: `bronze.nyc_taxi_bronze`
  - Silver: `silver.nyc_taxi_silver`

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
