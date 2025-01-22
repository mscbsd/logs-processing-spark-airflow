# Processamento de Logs com PySpark e Airflow

Este projeto demonstra um pipeline de processamento de logs armazenados no S3, utilizando PySpark para o processamento de dados e Apache Airflow para orquestração. O pipeline realiza as seguintes tarefas:

*   Contagem de acessos por hora.
*   Contagem de acessos únicos por IP.
*   Detecção de possíveis tentativas de acesso malicioso (IPs com mais de 1000 acessos em 1 hora).

Os resultados são salvos em formato Parquet no S3.

## Pré-requisitos

*   **Python 3.7+:** Certifique-se de ter o Python instalado.
*   **Apache Airflow:** Instale e configure o Airflow. Consulte a documentação oficial para obter instruções detalhadas: [https://airflow.apache.org/docs/](https://airflow.apache.org/docs/)
*   **PySpark:** Instale o PySpark. Uma forma comum é usando `pip install pyspark`. Certifique-se também de ter o Java instalado e configurado corretamente, pois o Spark depende do Java.
*   **Boto3:** Instale o boto3 para interagir com a AWS: `pip install boto3`
*   **Pendulum:** Instale o pendulum para lidar com datas e horários: `pip install pendulum`
*   **AWS Credentials:** Configure suas credenciais da AWS. A forma mais segura é usando variáveis de ambiente:
    *   `AWS_ACCESS_KEY_ID`
    *   `AWS_SECRET_ACCESS_KEY`
    *   `AWS_REGION` (opcional, padrão: us-east-1)
    *   `BUCKET_NAME`: O nome do seu bucket S3.
*   **Bucket S3:** Crie um bucket S3 para armazenar os logs de entrada e os relatórios de saída. Crie também uma pasta "logs" dentro desse bucket.
*   **Ambiente Spark:** Configure um ambiente Spark. Para testes locais, você pode usar `local[*]`, mas para produção, você precisará configurar um cluster Spark (YARN, Standalone, Kubernetes, etc.).

## Configuração

1.  **Clone o repositório:** (Se aplicável)
    ```bash
    git clone <seu_repositorio>
    cd <nome_do_projeto>
    ```

2.  **Copie os arquivos:**
    *   Copie o script `processamento_logs.py` e `dag_processamento_logs.py` para o diretório `dags` do seu Airflow (geralmente `$AIRFLOW_HOME/dags`).

3.  **Variáveis de Ambiente do Airflow:**
    Na interface web do Airflow (Admin -> Variables), crie as seguintes variáveis (ou defina-as no seu sistema operacional):

    *   `AWS_ACCESS_KEY_ID`: Sua chave de acesso AWS.
    *   `AWS_SECRET_ACCESS_KEY`: Sua chave secreta AWS.
    *   `AWS_REGION`: Sua região AWS (opcional).
    *   `BUCKET_NAME`: O nome do seu bucket S3.

4. **Dependências PySpark:**
    Certifique-se que o Airflow tenha acesso ao PySpark. Uma forma de fazer isso é instalando o pacote `apache-airflow-providers-apache-spark` no seu ambiente Airflow.

## Execução

1.  **Interface Web do Airflow:** Acesse a interface web do Airflow (geralmente em `http://localhost:8080`).
2.  **Ative a DAG:** Encontre a DAG chamada `processamento_logs_s3` na lista de DAGs e ative-a.
3.  **Execute a DAG:** Clique no botão "Trigger DAG" para executar a DAG manualmente. Ou aguarde a execução agendada (`@daily`).

## Arquivos

*   `processamento_logs.py`: Script PySpark que processa os logs.
*   `dag_processamento_logs.py`: DAG do Airflow que orquestra o processamento.

## Estrutura de diretórios no S3

*   `s3://<BUCKET_NAME>/logs/`: Pasta onde os arquivos de log CSV de entrada devem ser armazenados. Os arquivos devem seguir o padrão `log_AAAAMMDD*.csv` (ex: `log_20240728_100000.csv`, `log_20240728_110000.csv`).
*   `s3://<BUCKET_NAME>/relatorios/data=<AAAAMMDD>/`: Pasta onde os relatórios Parquet de saída serão armazenados, organizados por data.

## Observações

*   O script PySpark usa o modo `overwrite` ao salvar os relatórios. Se você precisar manter um histórico, considere usar o modo `append` ou particionar os dados por data.
*   O exemplo usa `spark-submit --master local[*]`. Para produção, configure o `--master` para o seu cluster Spark.
*   Adapte o padrão de nome de arquivo dos logs no script `processamento_logs.py` se necessário.
*   Este README fornece uma base. Adapte-o às suas necessidades específicas.

## Contribuições

Contribuições são bem-vindas!

## Licença

MIT
