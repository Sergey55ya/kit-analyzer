import pandas as pd
import requests

url = "https://docs.google.com/spreadsheets/d/1Cqr7WKBAaJqBjDBSU6flvouMtUKKO01Y/export?format=xlsx"

print("Скачиваю файл...")
response = requests.get(url)
with open("source.xlsx", "wb") as f:
    f.write(response.content)

print("Читаю файл...")
df = pd.read_excel("source.xlsx", sheet_name=0, header=None)

print("\nПЕРВЫЕ 30 СТРОК ИСХОДНОГО ФАЙЛА:")
print("=" * 80)
for i in range(min(30, len(df))):
    row = df.iloc[i].tolist()
    # Очищаем от nan
    row_clean = [str(x) if pd.notna(x) else "" for x in row]
    print(f"Строка {i+1:3d}: {row_clean}")
