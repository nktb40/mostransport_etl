# mostransport_etl

На сервере:

1. Копируем проект из гита

   git clone https://github.com/nktb40/mostransport_etl.git


Локально:

2. Создать архив с входными данными


3. Скопировать архив на сервер ETL
scp in.tar.xz etl_user@130.193.45.222:mostransport_etl/


На сервере ETL:

4. Заходим на сервер
ssh etl_user@130.193.45.222

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
scp etl_user@130.193.45.222:mostransport_etl/out.tar.xz mostransport_etl/

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
tippecanoe -zg -o out/stations/mbtiles/VLG-stations.mbtiles -l "bus_stops" -f out/stations/geojson/stations.geojson
tippecanoe -zg -o out/routes/mbtiles/PRM-routes.mbtiles -f out/routes/geojson/routes.geojson

tippecanoe -zg -o USH-density.mbtiles -l "density" -f USH-density.GeoJSON 

14. Загружаем векторные файлы в MapBox
