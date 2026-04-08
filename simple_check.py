import pandas as pd
import requests

print("ПРОСТАЯ ПРОВЕРКА НАЛИЧИЯ")
print("=" * 50)

# Скачиваем компоненты
print("1. Скачиваю компоненты...")
r = requests.get("https://admin.silam.ru/system/unload_prices/29/zzap_copy_1.xlsx?rand=be27db11-7b4b-4e97-8e70-7168420fc4af")
with open("comp.xlsx", "wb") as f:
    f.write(r.content)

# Читаем компоненты
df = pd.read_excel("comp.xlsx", sheet_name=0)
print(f"   Всего строк: {len(df)}")
print(f"   Колонки: {df.columns.tolist()}")

# Проверяем колонку с артикулами
if 'Код' in df.columns:
    print(f"\n2. Примеры артикулов из файла компонентов:")
    for i in range(min(5, len(df))):
        print(f"   {df.iloc[i]['Код']}")
    
    # Проверяем конкретный артикул из комплекта
    test_article = "PMT5193-12"
    print(f"\n3. Ищу артикул: {test_article}")
    
    found = df[df['Код'].astype(str) == test_article]
    if found.empty:
        # Ищем похожие
        found = df[df['Код'].astype(str).str.contains("PMT5193", na=False)]
    
    if not found.empty:
        print(f"   ✅ НАЙДЕН!")
        print(f"   Наличие: {found.iloc[0]['Наличие'] if 'Наличие' in found.columns else 'нет данных'}")
        print(f"   Цена: {found.iloc[0]['Цена'] if 'Цена' in found.columns else 'нет данных'}")
    else:
        print(f"   ❌ НЕ НАЙДЕН")
        
        # Показываем все артикулы, начинающиеся на PMT
        pmt_codes = df[df['Код'].astype(str).str.startswith('PMT', na=False)]
        if not pmt_codes.empty:
            print(f"\n   Найденные артикулы на PMT:")
            for code in pmt_codes['Код'].head(10):
                print(f"      {code}")

print("\n" + "=" * 50)
print("Если артикулы не найдены, значит нужно:")
print("1. Убрать дефисы из артикулов компонентов")
print("2. Или нормализовать артикулы при поиске")
