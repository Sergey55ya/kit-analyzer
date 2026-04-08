# analyze_kits.py - Анализатор складских комплектов
import pandas as pd
import requests
from datetime import datetime
import re
from collections import defaultdict
import os

print("🚀 ЗАПУСК АНАЛИЗА")
print("="*50)

# Ссылки на файлы
COMPONENTS_URL = "https://admin.silam.ru/system/unload_prices/29/zzap_copy_1.xlsx?rand=be27db11-7b4b-4e97-8e70-7168420fc4af"
KITS_URL = "https://docs.google.com/spreadsheets/d/1Cqr7WKBAaJqBjDBSU6flvouMtUKKO01Y/export?format=xlsx"

def download_file(url, filename):
    """Скачивает файл"""
    print(f"Скачиваю {filename}...")
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"✅ {filename} скачан")
            return True
    except Exception as e:
        print(f"❌ Ошибка: {e}")
    return False

def normalize_article(article):
    """Очищает артикул"""
    if not isinstance(article, str):
        article = str(article)
    article = article.replace(' ', '').replace('-', '')
    article = re.sub(r'[^A-Za-z0-9]', '', article)
    return article.upper()

# Скачиваем файлы
download_file(COMPONENTS_URL, "components.xlsx")
download_file(KITS_URL, "kits.xlsx")

# Загружаем компоненты
print("\nЗагружаю компоненты...")
df_components = pd.read_excel("components.xlsx", sheet_name=0)
df_components['Код'] = df_components['Код'].astype(str).str.strip()
df_components['Код_норм'] = df_components['Код'].apply(normalize_article)
df_components['Цена'] = pd.to_numeric(df_components['Цена'], errors='coerce')
df_components['Наличие'] = pd.to_numeric(df_components['Наличие'], errors='coerce').fillna(0)
print(f"✅ Загружено {len(df_components)} компонентов")

# Загружаем комплекты
print("\nЗагружаю комплекты...")
df_kits = pd.read_excel("kits.xlsx", sheet_name=0, header=None)

kits = {}
current_kit = None
kit_name = ""
kit_components = []

for i in range(len(df_kits)):
    row = df_kits.iloc[i].astype(str).tolist()
    
    if len(row) > 1 and row[1] == 'Комплект':
        if current_kit and kit_components:
            kits[current_kit] = {
                'name': kit_name,
                'components': list(set(kit_components))
            }
        
        if i + 1 < len(df_kits):
            next_row = df_kits.iloc[i+1].astype(str).tolist()
            if len(next_row) > 2:
                kit_name = next_row[1]
                current_kit = next_row[2]
                kit_components = []
    
    elif current_kit and len(row) > 2:
        article = row[2].strip()
        if article and article != 'nan' and len(article) > 2:
            kit_components.append(article)

# Сохраняем последний комплект
if current_kit and kit_components:
    kits[current_kit] = {
        'name': kit_name,
        'components': list(set(kit_components))
    }

print(f"✅ Загружено {len(kits)} комплектов")

# Анализ комплектов
print("\n🔍 Анализирую комплекты...")
results = []

for kit_article, kit_info in kits.items():
    print(f"  Анализ: {kit_article}")
    
    min_available = float('inf')
    
    for component in kit_info['components']:
        comp_norm = normalize_article(component)
        found = df_components[df_components['Код_норм'] == comp_norm]
        
        if found.empty:
            min_available = 0
            break
        
        total_available = found['Наличие'].sum()
        if total_available < min_available:
            min_available = total_available
    
    # Формируем результат как в примере
    results.append({
        'Комплект': kit_info['name'],
        'Артикул': kit_article,
        'Бренд': 'PowerMechanics',
        'Количество': '',
        'Цена': '',
        'Срок': ''
    })
    
    results.append({
        'Комплект': '',
        'Артикул': '',
        'Бренд': '',
        'Количество': '',
        'Цена': '',
        'Срок': ''
    })
    
    results.append({
        'Комплект': kit_info['name'],
        'Артикул': kit_article,
        'Бренд': 'PowerMechanics',
        'Количество': 0,
        'Цена': '—',
        'Срок': '—'
    })
    
    results.append({
        'Комплект': kit_info['name'],
        'Артикул': kit_article,
        'Бренд': 'PowerMechanics',
        'Количество': 0,
        'Цена': '—',
        'Срок': '—'
    })
    
    if min_available > 0 and min_available != float('inf'):
        results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': int(min_available),
            'Цена': '—',
            'Срок': '—'
        })
    else:
        results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': 0,
            'Цена': '—',
            'Срок': '—'
        })
    
    results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
    results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})

# Сохраняем результат
output_file = f'results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
df_results = pd.DataFrame(results)
df_results.to_csv(output_file, index=False, encoding='utf-8-sig')

print(f"\n✅ ГОТОВО!")
print(f"📊 Результат сохранен в файл: {output_file}")
print(f"📈 Проанализировано комплектов: {len(kits)}")
print(f"\n📋 ПЕРВЫЕ 5 СТРОК РЕЗУЛЬТАТА:")
print(df_results.head(10).to_string())
