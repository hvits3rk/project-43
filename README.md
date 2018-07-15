Пакуем приложение в `jar` файл
```shell
mvn clean package
```
> `jar` файл будет сохранен в папку `deploy` с именем `app.jar`

Создаем докер-образ со спарком
```shell
docker image build -t hvits3rk/spark:latest .
```

Запускаем контейнер
```shell
docker container run -it --rm \
    -p 8080:8080 \
    -p 4040:4040 \
    -v ~/Documents/dev/spark-app/input:/app/input \
    -v ~/Documents/dev/spark-app/output:/app/output \
    hvits3rk/spark:latest
```
> `~/Documents/dev/spark-app/input` в эту директорию ложим `csv` файл
>
> `~/Documents/dev/spark-app/output` а в эту уйдет результат работы приложения

В терминале контейнера запускаем мастера и слейва спарка
```shell
/usr/local/spark/sbin/start-master.sh \
&& /usr/local/spark/sbin/start-slave.sh spark://localhost:7077
```

Сабмитим наше приложение спарку
```shell
spark-submit \
    --class com.romantupikov.Main \
    /app/app.jar \
    /app/input/temp.csv
```
>`temp.csv` это переименнованных файл `GlobalLandTemperaturesByCity.csv` скачанный с 
[www.kaggle.com](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).

> При создании докер-образа, `jar` файл копируется из папки `deploy`
в папку `/app/app.jar`.

>Предусмотрен `volume` `/app`, который можно привязать,
например к той же папке `deploy`
>
>`-v ~/Documents/dev/spark-app/deploy:/app`

> По умолчанию запускаться будут два слейва по 2 ядра и 2гб памяти каждый.
Можно переопределить с помощью переменных
`SPARK_WORKER_CORES` `SPARK_WORKER_MEMORY` `SPARK_WORKER_INSTANCES`

>Например:
>```shell
>docker container run -it --rm \
>     -p 8080:8080 \
>     -p 4040:4040 \
>     -v ~/Documents/dev/spark-app/input:/app/input \
>     -v ~/Documents/dev/spark-app/output:/app/output \
>     -v ~/Documents/dev/spark-app/deploy:/app \
>     -e "SPARK_WORKER_CORES=4" \
>     -e "SPARK_WORKER_MEMORY=4g" \
>     -e "SPARK_WORKER_INSTANCES=1" \
>     hvits3rk/spark:latest
>```

> При запуске мастера, станет доступен веб-интерфейс.
По адресу [http://localhost:8080](http://localhost:8080)
