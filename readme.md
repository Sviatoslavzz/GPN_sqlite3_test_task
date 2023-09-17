## Тестовое задание ГПН ЦПС
***
### Скрипт собирает, анализирует и приводит к необходимому формату исторические данные из файлов excel в формате _.xlsx_ затем загружает отформатированные данные в базы данных _sqlite_
***
#### Вводные данные тестового задания:
1. На вход поступают одинаковые по структуре файлы в формате xlsx: количество и название колонок фиксированное, количество строк с данными различно.
2. В источниках могут содержаться ошибки типов данных, пустые ячейки, ячейки с ошибками НД. Ошибки не должны попадать в таблицы БД.
3. Источники приходят пакетами, раз в месяц/неделю.
***
#### Подробное описание работы скрипта / требования к источникам:
1. Источники должны быть помещены в папку _"/sources"_ в той же директории, где находится исполнямый файл
2. Источники должны иметь идентичный порядок расположения колонок и заголовков. Строк с данными может быть неограниченное кол-во.
3. Производится проверка данных и заголовков.
4. Собираются датафреймы с данными при помощи библиотеки _pandas_. Всего собирается 13 дф для каждого файла. Данные за год и за 12 месяцев (отдельно по каждому месяцу).
5. Готовые дф с данными загружаются в _sqlite_ локальные файлы с тем же названием, что у исходного файла _.xlsx_
6. Базы данных с расширением _.db_ сохраняются в той же директории в папке _"databases"_.

p.s. в текущем сетапе проекта источники и базы данных отсутствуют в папках, необходимо загрузить их самостоятельно.