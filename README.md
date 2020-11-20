# mostransport_etl

На сервере:

1. Копируем проект из гита

   git clone https://github.com/nktb40/mostransport_etl.git


Локально:

2. Создать архив с входными данными
   2.1. В файле параметров (/in/params/params.json) указать:
    - Код города, который планируется загружать
    - Интервал движения ОТ по умолчанию
    - Кол-во потоков для распараллеливания расчётов
      {
         "city_code":"SPB",
         "default_interval":10,
         "threads_num": 6
      }
   
   2.2. Создать .tar архив из папки "in" (in.tar.xz). В архив должны быть помещена вся папка целиком 

3. Скопировать архив на сервер ETL
scp in.tar.xz etl_user@130.193.45.222:mostransport_etl/


На сервере ETL:

4. Заходим на сервер
ssh etl_user@130.193.45.222
cd mostransport_etl

5.1. Удаляем папку "in" со старыми данными
rm -rf in

5.2. Разархивируем папку с входными данными
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


Локально:

11. Генерируем векторные файлы для остановок и маршрутов (вместо USH подставить код города)
tippecanoe -zg -o out/stations/mbtiles/USH-stations.mbtiles -l "bus_stops" -f out/stations/geojson/stations.geojson
tippecanoe -zg -o out/routes/mbtiles/USH-routes.mbtiles -f out/routes/geojson/routes.geojson

---- Выполнять не надо. Просто чтобы не потерять
tippecanoe -zg -o EKB-density.mbtiles -l "density" -f ekb_density.geojson

12. Загружаем векторные файлы в MapBox
На выходе получим url загруженных tileset-файлов вида "nktb.22k1ka1u"

На сервере Mostransport:

13. Разархивируем папку с выходными данными
cur
cd seeds/

tar -xf out.tar.xz

cd ..

mv -v seeds/out/* seeds/

rm -rf seeds/out

rm seeds/out.tar.xz

14. Добавляем информацию о новом городе в файл db/seeds.rb

14.1. nano db/seeds.rb

14.2. Находим раздел Cities. Добавляем строку в массив items вида

items = [
  ...
  {name: "Москва", code: "MSK", tile_stations_url: 'nktb.bev2q4f8', tile_routes_url: 'nktb.4pzcpmcq', longitude:37.6092076, latitude: 55.7548403}
]

14.3 Сохраняем файл командой CTRL+o 
     и выходим из редактора CTRL+x

15. Запускам импорт данных:
bundle exec rails db:seed

