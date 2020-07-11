# mostransport_etl
1. Если на сервере нет папки с проектом, то копируем проект из гита

	git clone git@github.com:nktb40/mostransport_etl.git
  
   Если проект уже на сервере, то выполняем обновление 

    git pull origin master


Локально:

2. Создать архив с входными данными


3. Скопировать архив на сервер
scp in.tar.xz etl_user@130.193.35.13:mostransport_etl/


На сервере:

4. Заходим на сервер
ssh etl_user@130.193.35.13

5. Разархивируем папку с входными данными
tar -xf in.tar.xz

6. Запускаем расчёт
cd isochrones3/scripts
nohup python3 -u isochrones_v9.py > output.log &

7. Просмотр состояния загрузки в логах:

tail -f output.log 

8. После окончания загрузки формируем архив с папкой out
cd ..
tar cvzf out.tar.xz out/


Локально:

9. Скачивваем архив с результатами с сервера
scp etl_user@130.193.35.13:mostransport_etl/out.tar.xz mostransport_etl/