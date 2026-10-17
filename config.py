ENDPOINT = "http://localhost:9000"
ACCESS_KEY = "YOUR_ACCESS_KEY"
SECRET_KEY = "YOUR_SECRET_KEY"

BUCKET = "hh-lake"
OBJECT_KEY = "cpp_vacancies.csv"

CSV_SEP = "\t"
CSV_ENCODING = "utf-8-sig"

# файл всего 20 мб , для наглядности взят маленький размер сплита
SPLIT = 4 * 1024 * 1024
REDUCERS = 8