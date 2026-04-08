import pandas as pd
import requests

print("ПРОВЕРКА ФАЙЛА КОМПОНЕНТОВ")
print("=" * 60)

# Скачиваем
url = "https://admin.silam.ru/system/unload_prices/29/zzap_copy_1.xlsx?rand=be27db11-7b4b-4e97-8e70-7168420fc4af"
response = requests.get(url)
with open("comp.xlsx", "wb") as f:
    f.write(response.content)

# Читаем
df = pd.read_excel("comp.xlsx", sheet_name=0)

print(f"Всего строк: {len(df)}")
print(f"\nНазвания колонок: {df.columns.tolist()}")
print(f"\nТипы данных:")
for col in df.columns:
    print(f"  {col}: {df[col].dtype}")

# Проверяем цены
if 'Цена' in df.columns:
    print(f"\nПроверка колонки 'Цена':")
    print(f"  Пустых значений: {df['Цена'].isna().sum()}")
    print(f"  Значение 0: {(df['Цена'] == 0).sum()}")
    print(f"  Отрицательных: {(df['Цена'] < 0).sum()}")
    print(f"  Положительных: {(df['Цена'] > 0).sum()}")
    print(f"\n  Примеры цен (первые 10):")
    for i in range(min(10, len(df))):
        print(f"    {i+1}. {df.iloc[i]['Код']}: цена = {df.iloc[i]['Цена']}")
else:
    print("\n❌ Колонка 'Цена' не найдена!")
    print("Ищу похожие колонки:")
    for col in df.columns:
        if 'цен' in col.lower() or 'price' in col.lower():
            print(f"  → {col}")

# Проверяем наличие
if 'Наличие' in df.columns:
    print(f"\nПроверка колонки 'Наличие':")
    print(f"  Сумма наличия: {df['Наличие'].sum()}")
    print(f"  Примеры наличия (первые 10):")
    for i in range(min(10, len(df))):
        print(f"    {i+1}. {df.iloc[i]['Код']}: наличие = {df.iloc[i]['Наличие']}")
else:
    print("\n❌ Колонка 'Наличие' не найдена!")

# Показываем первые 5 строк полностью
print(f"\nПЕРВЫЕ 5 СТРОК (все колонки):")
for i in range(min(5, len(df))):
    print(f"\nСтрока {i+1}:")
    for col in df.columns:
        print(f"  {col}: {df.iloc[i][col]}")
