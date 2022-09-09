# appmax_desafio
Resolução do case para data engineer jr

# Passo a passo

Para iniciar você deve clonar esse repositório em sua máquina.


```
git clone link

```
A estrutura de pastas já está toda pronta, então basta construir a imagem do dockerfile, para fazer isso utilize o seguinte comando:

```
docker build .

```

Antes de inicializar tudo temos que fazer o seguinte comando, para criar as credências do airflow, segundo a documentação oficial.
```
docker-compose -f docker-compose.airflow.yaml up airflow-init

```

Agora podemos inicializar tudo
```
docker-compose -f docker-compose.spark.yaml -f docker-compose.airflow.yaml up

```

## Airflow

Para acessar o Airflow basta ir até **localhost:8080** no seu navegador. Irá abrir uma tela de login do airflow.

![Tela de login airflow](./auxiliares/airflow_login.png)

```
Usuário: airflow
Senha: airflow
```
## Spark

Para acessar o Spark basta ir até **localhost:9090** no seu navegador. Neste endereço pegamos o endereço do spark para fazer a conexão com o airflow.

No meu caso foi spark://ec0cc1435450:7077 

## Jupyter Notebook

Para acessar o jupyter notebook basta ir até **localhost:8888** no seu navegador, irá abrir uma tela pedindo a senha, basta colocar a palavra tad.
![Tela de login airflow](./auxiliares/jupyter_token.png)

# Explicações e decisões

O arquivo docker-compose.airflow foi extraido do [site oficial](https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml), mas com algumas modificações para podermos ter acesso ao spark. Porque por padrão o airflow não tem acesso ao spark.

Para configurar a conexão devemos fazer o login no airflow, clicar em admin e depois conections e configurar nossa conexão. Configurei a minha com os parametros:

* Connection Id = conexao_spark (Esse nome será utilizado nas dags)
* Connection Type = Spark (O Spark não estará disponível se você não fizer o docker build)
* Host = spark://ec0cc1435450 (Podemos pegar esse caminho ao acessar o spark)
* Port = 7077 (Números após os dois pontos no caminho do spark)

![Tela de login airflow](./auxiliares/conexao_spark.png)

# Problemas e pontos a melhorar

