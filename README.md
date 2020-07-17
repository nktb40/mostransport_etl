# mostransport_etl

* Вместо 130.193.35.13 подставлять IP ETL сервера

1. Если на сервере ETL нет папки mostransport_etl с проектом, то копируем проект из гита

	git clone https://github.com/nktb40/mostransport_etl.git
  
   Если проект уже на сервере, то выполняем обновление 

    git pull origin master


Локально:

2. Создать архив с входными данными


3. Скопировать архив на сервер ETL
scp in.tar.xz etl_user@130.193.35.13:mostransport_etl/


На сервере ETL:

4. Заходим на сервер
ssh etl_user@130.193.35.13

5. Разархивируем папку с входными данными
tar -xf in.tar.xz

6. Запускаем расчёт
cd scripts
nohup python3 -u index.py > output.log &

7. Просмотр состояния загрузки в логах:

tail -f output.log

8. После окончания загрузки формируем архив с папкой out
cd ..
tar cvzf out.tar.xz out/


Локально:

9. Скачивваем архив с результатами с сервера
scp etl_user@130.193.35.13:mostransport_etl/out.tar.xz mostransport_etl/

10. Отправляем архив на сервер mostransport
scp out.tar.xz mostransport@mostransport.info:mostransport/current/seeds


На сервере Mostransport:

11. Разархивируем папку с выходными данными
cur
cd seeds/
tar -xf out.tar.xz
cd ..
mv -v seeds/out/* seeds/
rm -rf seeds/out
rm seeds/out.tar.xz

12. Запускам импорт данных:
bundle exec rails db:seed

Локально:

13. Генерируем векторные файлы для остановок и маршрутов (вместо USH подставить код города)
tippecanoe -o out/stations/mbtiles/USH-stations.mbtiles -l "bus_stops" -f out/stations/geojson/stations.geojson
tippecanoe -o out/routes/mbtiles/USH-routes.mbtiles -f out/routes/geojson/routes.geojson

14. Загружаем векторные файлы в MapBox
