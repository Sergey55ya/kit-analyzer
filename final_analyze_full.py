# final_analyze_full.py
import pandas as pd
import requests
from datetime import datetime
import re
from collections import defaultdict

print("🚀 ФИНАЛЬНЫЙ АНАЛИЗ (ОБА ФАЙЛА + НОРМАЛИЗАЦИЯ)")
print("="*60)

# Ссылки на файлы
COMPONENTS_URL = "https://admin.silam.ru/system/unload_prices/29/zzap_copy_1.xlsx?rand=c5149f53-bd66-469f-880f-534e624f837a"
KITS_URL = "https://docs.google.com/spreadsheets/d/1Cqr7WKBAaJqBjDBSU6flvouMtUKKO01Y/export?format=xlsx"

def download_file(url, filename):
    print(f"📥 Скачиваю {filename}...")
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"   ✅ {filename} скачан ({len(response.content)} байт)")
            return True
        else:
            print(f"   ❌ Ошибка HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
        return False

def normalize_article(article):
    """Удаляет дефисы, пробелы и приводит к верхнему регистру"""
    if pd.isna(article):
        return ""
    article = str(article).strip()
    article = article.replace('-', '').replace(' ', '')
    article = re.sub(r'[^A-Za-z0-9]', '', article)
    return article.upper()

# Скачиваем файлы
print("\n1. ЗАГРУЗКА ФАЙЛОВ")
print("-"*40)
download_file(COMPONENTS_URL, "components.xlsx")
download_file(KITS_URL, "kits.xlsx")

# Загружаем компоненты (если файл скачался)
print("\n2. ЗАГРУЗКА КОМПОНЕНТОВ")
print("-"*40)
try:
    df_components = pd.read_excel("components.xlsx", sheet_name=0)
    df_components['Код'] = df_components['Код'].astype(str).str.strip()
    df_components['Код_норм'] = df_components['Код'].apply(normalize_article)
    df_components['Наличие'] = pd.to_numeric(df_components['Наличие'], errors='coerce').fillna(0)
    df_components = df_components[df_components['Цена'] > 0]  # оставляем только товары с ценой
    print(f"   ✅ Загружено компонентов: {len(df_components)}")
    print(f"   📝 Примеры артикулов: {df_components['Код'].head(3).tolist()}")
    components_available = True
except Exception as e:
    print(f"   ❌ Не удалось загрузить компоненты: {e}")
    components_available = False
    df_components = pd.DataFrame()

# Загружаем и парсим комплекты
print("\n3. ЗАГРУЗКА КОМПЛЕКТОВ")
print("-"*40)
df_kits = pd.read_excel("kits.xlsx", sheet_name=0, header=None)
print(f"   ✅ Всего строк в файле комплектов: {len(df_kits)}")

# Парсинг комплектов (как раньше)
kits = {}
current_kit = None
current_kit_name = ""
current_components = []
reading_components = False

for i in range(len(df_kits)):
    row = df_kits.iloc[i].astype(str).tolist()
    while len(row) < 7:
        row.append("")
    
    if len(row) > 1 and row[1] == "Комплект":
        if current_kit and current_components:
            kits[current_kit] = {
                'name': current_kit_name,
                'components': list(set(current_components))
            }
            print(f"   ✅ {current_kit}: {len(set(current_components))} компонентов")
        
        if i + 1 < len(df_kits):
            next_row = df_kits.iloc[i + 1].astype(str).tolist()
            if len(next_row) > 2:
                current_kit_name = next_row[1] if next_row[1] != "nan" else ""
                current_kit = next_row[2] if next_row[2] != "nan" else ""
                current_components = []
                reading_components = False
        continue
    
    if len(row) > 1 and row[1] == "Наименование":
        reading_components = True
        continue
    
    if reading_components and current_kit:
        component_article = row[2] if len(row) > 2 else ""
        if component_article and component_article != "nan" and component_article != "":
            skip_words = ["гофроящик", "этикетка", "ложемент", "упаковка", "коробка", "бренд"]
            if not any(word in component_article.lower() for word in skip_words):
                if len(component_article) > 2:
                    current_components.append(component_article)

if current_kit and current_components:
    kits[current_kit] = {
        'name': current_kit_name,
        'components': list(set(current_components))
    }
    print(f"   ✅ {current_kit}: {len(set(current_components))} компонентов")

print(f"\n✅ ВСЕГО НАЙДЕНО КОМПЛЕКТОВ: {len(kits)}")

# Анализ наличия
print("\n4. АНАЛИЗ НАЛИЧИЯ КОМПЛЕКТОВ")
print("="*60)

if not components_available or df_components.empty:
    print("❌ Нет данных о компонентах. Проверьте ссылку на components.xlsx")
    results = []
    for kit_article, kit_info in kits.items():
        results.append({'Комплект': kit_info['name'], 'Артикул': kit_article, 'Бренд': 'PowerMechanics', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': kit_info['name'], 'Артикул': kit_article, 'Бренд': 'PowerMechanics', 'Количество': 0, 'Цена': '—', 'Срок': '—'})
        results.append({'Комплект': kit_info['name'], 'Артикул': kit_article, 'Бренд': 'PowerMechanics', 'Количество': 0, 'Цена': '—', 'Срок': '—'})
        results.append({'Комплект': kit_info['name'], 'Артикул': kit_article, 'Бренд': 'PowerMechanics', 'Количество': 0, 'Цена': '—', 'Срок': '—'})
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
else:
    results = []
    kits_with_stock = 0
    
    for kit_article, kit_info in kits.items():
        print(f"\n📦 {kit_article}: {kit_info['name'][:50]}")
        
        min_available = float('inf')
        missing = []
        
        for component in kit_info['components']:
            # Пробуем найти компонент разными способами
            comp_norm = normalize_article(component)
            
            # Сначала ищем по нормализованному коду
            found = df_components[df_components['Код_норм'] == comp_norm]
            
            # Если не нашли, ищем по точному совпадению
            if found.empty:
                found = df_components[df_components['Код'] == component]
            
            # Если всё равно не нашли, ищем по частичному совпадению
            if found.empty and len(comp_norm) > 5:
                found = df_components[df_components['Код_норм'].str.contains(comp_norm[:5], na=False)]
            
            if found.empty:
                missing.append(component)
                continue
            
            total_available = found['Наличие'].sum()
            if total_available < min_available:
                min_available = total_available
        
        if min_available == float('inf'):
            min_available = 0
        
        if min_available > 0:
            kits_with_stock += 1
            print(f"   ✅ МОЖНО СОБРАТЬ: {int(min_available)} шт.")
        else:
            print(f"   ❌ НЕЛЬЗЯ СОБРАТЬ (пропущено: {len(missing)})")
            if missing[:3]:
                print(f"      Например: {missing[:3]}")
        
        # Формируем вывод
        results.append({'Комплект': kit_info['name'], 'Артикул': kit_article, 'Бренд': 'PowerMechanics', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': kit_info['name'], 'Артикул': kit_article, 'Бренд': 'PowerMechanics', 'Количество': 0, 'Цена': '—', 'Срок': '—'})
        results.append({'Комплект': kit_info['name'], 'Артикул': kit_article, 'Бренд': 'PowerMechanics', 'Количество': 0, 'Цена': '—', 'Срок': '—'})
        
        if min_available > 0:
            results.append({'Комплект': kit_info['name'], 'Артикул': kit_article, 'Бренд': 'PowerMechanics', 'Количество': int(min_available), 'Цена': '—', 'Срок': '—'})
        else:
            results.append({'Комплект': kit_info['name'], 'Артикул': kit_article, 'Бренд': 'PowerMechanics', 'Количество': 0, 'Цена': '—', 'Срок': '—'})
        
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
    
    print(f"\n📈 Комплектов в наличии: {kits_with_stock} из {len(kits)}")

# Сохраняем результат
output_file = f'results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
df_results = pd.DataFrame(results)
df_results.to_csv(output_file, index=False, encoding='utf-8-sig')

print("\n" + "="*60)
print(f"✅ ГОТОВО! Результат: {output_file}")
