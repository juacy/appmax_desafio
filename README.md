# appmax_desafio
Resolução do case para data engineer jr

Passo a passo

Para iniciar você deve clonar esse repositório em sua máquina.

git clone link

docker build .

docker-compose -f docker-compose.airflow.yaml up airflow-init

docker-compose -f docker-compose.spark.yaml -f docker-compose.airflow.yaml up

localhost:8080 -> airflow
![alt text](appmax_desafio/auxiliares/airflow_login.png)
usuário:airflow
senha:airflow

localhost:9090 -> spark
Entrando lá podemos pegar o endereço do spark para fazer a conexão com o airflow
spark://ec0cc1435450:7077 

localhost:8888 -> jupyter notebook

Com o comando 

docker exec -it teste_de_jupyter-base-notebook_1 bash

Entramos no jupyter notebook

http://4832ddd8a100:8888/?token=tad :: /home/jovyan

Mas a senha é tad

Problemas

Pontos a melhorar
