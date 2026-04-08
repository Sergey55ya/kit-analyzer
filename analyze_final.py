import pandas as pd
import requests
from datetime import datetime
import re
from collections import defaultdict

print("🚀 ЗАПУСК АНАЛИЗА (ОТЛАДОЧНАЯ ВЕРСИЯ)")
print("=" * 60)

# Ссылки на файлы
COMPONENTS_URL = "https://admin.silam.ru/system/unload_prices/29/zzap_copy_1.xlsx?rand=be27db11-7b4b-4e97-8e70-7168420fc4af"
KITS_URL = "https://docs.google.com/spreadsheets/d/1Cqr7WKBAaJqBjDBSU6flvouMtUKKO01Y/export?format=xlsx"

def download_file(url, filename):
    """Скачивает файл"""
    print(f"📥 Скачиваю {filename}...")
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"   ✅ {filename} скачан ({len(response.content)} байт)")
            return True
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
    return False

def normalize_article(article):
    """Очищает артикул"""
    if pd.isna(article):
        return ""
    article = str(article).strip()
    if article.startswith("'"):
        article = article[1:]
    article = article.replace(' ', '').replace('-', '')
    article = re.sub(r'[^A-Za-z0-9]', '', article)
    return article.upper()

# Скачиваем файлы
download_file(COMPONENTS_URL, "components.xlsx")
download_file(KITS_URL, "kits.xlsx")

# ============================================
# ОТЛАДКА: Проверяем компоненты
# ============================================
print("\n" + "=" * 60)
print("ОТЛАДКА: ПРОВЕРКА ФАЙЛА КОМПОНЕНТОВ")
print("=" * 60)

df_comp = pd.read_excel("components.xlsx", sheet_name=0)
print(f"Всего строк в компонентах: {len(df_comp)}")
print(f"Колонки: {df_comp.columns.tolist()}")

if 'Код' in df_comp.columns:
    print(f"\nПервые 5 артикулов из компонентов:")
    for i in range(min(5, len(df_comp))):
        print(f"  {i+1}. {df_comp.iloc[i]['Код']}")
else:
    print("❌ Колонка 'Код' не найдена!")
    
# Проверяем наличие колонки 'Наличие'
if 'Наличие' in df_comp.columns:
    print(f"\nПримеры наличия:")
    for i in range(min(5, len(df_comp))):
        print(f"  {df_comp.iloc[i]['Код']}: наличие = {df_comp.iloc[i]['Наличие']}")
else:
    print("❌ Колонка 'Наличие' не найдена!")

# ============================================
# Загружаем компоненты для анализа
# ============================================
print("\n" + "=" * 60)
print("ЗАГРУЗКА КОМПОНЕНТОВ ДЛЯ АНАЛИЗА")
print("=" * 60)

df_components = pd.read_excel("components.xlsx", sheet_name=0)
df_components['Код'] = df_components['Код'].astype(str).str.strip()
df_components['Код_норм'] = df_components['Код'].apply(normalize_article)
df_components['Наличие'] = pd.to_numeric(df_components['Наличие'], errors='coerce').fillna(0)
df_components['Цена'] = pd.to_numeric(df_components['Цена'], errors='coerce').fillna(0)
df_components = df_components[df_components['Цена'] > 0]
print(f"✅ Загружено {len(df_components)} компонентов (с ценой > 0)")

# ============================================
# Загружаем комплекты
# ============================================
print("\n" + "=" * 60)
print("ЗАГРУЗКА КОМПЛЕКТОВ")
print("=" * 60)

df_kits = pd.read_excel("kits.xlsx", sheet_name=0, header=None)

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

if len(kits) == 0:
    print("\n⚠️ КОМПЛЕКТЫ НЕ НАЙДЕНЫ!")
else:
    # ============================================
    # ОТЛАДКА: Проверяем первый комплект
    # ============================================
    print("\n" + "=" * 60)
    print("ОТЛАДКА: ПРОВЕРКА ПЕРВОГО КОМПЛЕКТА")
    print("=" * 60)
    
    first_kit = list(kits.keys())[0]
    first_components = kits[first_kit]['components']
    print(f"Комплект: {first_kit}")
    print(f"Название: {kits[first_kit]['name']}")
    print(f"Первые 5 компонентов: {first_components[:5]}")
    
    print(f"\nПроверка наличия этих компонентов на складе:")
    for comp in first_components[:5]:
        comp_norm = normalize_article(comp)
        found = df_components[df_components['Код_норм'] == comp_norm]
        
        if found.empty:
            print(f"  ❌ {comp} -> НЕ НАЙДЕН")
            # Ищем похожий
            similar = df_components[df_components['Код'].str.contains(comp[:5], case=False, na=False)]
            if not similar.empty:
                print(f"     Похожие: {similar['Код'].tolist()[:2]}")
        else:
            stock = found.iloc[0]['Наличие']
            price = found.iloc[0]['Цена']
            print(f"  ✅ {comp} -> НАЙДЕН! Наличие: {stock}, Цена: {price}")
    
    # ============================================
    # Анализ всех комплектов
    # ============================================
    print("\n" + "=" * 60)
    print("АНАЛИЗ ВСЕХ КОМПЛЕКТОВ")
    print("=" * 60)
    
    results = []
    
    for kit_article, kit_info in kits.items():
        print(f"\n📦 {kit_article}: {kit_info['name'][:40]}")
        
        min_available = float('inf')
        limiting_component = None
        
        for component in kit_info['components']:
            comp_norm = normalize_article(component)
            found = df_components[df_components['Код_норм'] == comp_norm]
            
            if found.empty:
                min_available = 0
                limiting_component = component
                print(f"   ❌ Не найден компонент: {component}")
                break
            
            total_available = found['Наличие'].sum()
            if total_available < min_available:
                min_available = total_available
                limiting_component = component
        
        if min_available == float('inf'):
            min_available = 0
        
        print(f"   Максимально комплектов: {min_available}")
        
        # Формируем результат
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
    
    # Сохраняем результат
    output_file = f'results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    df_results = pd.DataFrame(results)
    df_results.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ ГОТОВО!")
    print(f"📊 Результат: {output_file}")

print("\n" + "=" * 60)
