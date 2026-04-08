import pandas as pd
import requests
from datetime import datetime
import re
from collections import defaultdict

print("🚀 ЗАПУСК АНАЛИЗА (ФИНАЛЬНАЯ ВЕРСИЯ)")
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
            print(f"   ✅ {filename} скачан")
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

# Загружаем компоненты
print("\n📄 Загружаю компоненты...")
df_components = pd.read_excel("components.xlsx", sheet_name=0)
df_components['Код'] = df_components['Код'].astype(str).str.strip()
df_components['Код_норм'] = df_components['Код'].apply(normalize_article)
df_components['Наличие'] = pd.to_numeric(df_components['Наличие'], errors='coerce').fillna(0)
df_components['Цена'] = pd.to_numeric(df_components['Цена'], errors='coerce').fillna(0)
df_components = df_components[df_components['Цена'] > 0]
print(f"   ✅ Загружено {len(df_components)} компонентов")

# Загружаем и парсим комплекты
print("\n📋 Парсинг комплектов...")
df_kits = pd.read_excel("kits.xlsx", sheet_name=0, header=None)

kits = {}
current_kit = None
current_kit_name = ""
current_components = []
reading_components = False

for i in range(len(df_kits)):
    row = df_kits.iloc[i].astype(str).tolist()
    
    # Заполняем пустые значения
    while len(row) < 7:
        row.append("")
    
    # Ищем строку с "Комплект"
    if len(row) > 1 and row[1] == "Комплект":
        # Сохраняем предыдущий комплект
        if current_kit and current_components:
            kits[current_kit] = {
                'name': current_kit_name,
                'components': list(set(current_components))  # удаляем дубликаты
            }
            print(f"   ✅ {current_kit}: {len(set(current_components))} компонентов")
        
        # Начинаем новый комплект (следующая строка содержит название и артикул)
        if i + 1 < len(df_kits):
            next_row = df_kits.iloc[i + 1].astype(str).tolist()
            if len(next_row) > 2:
                current_kit_name = next_row[1] if next_row[1] != "nan" else ""
                current_kit = next_row[2] if next_row[2] != "nan" else ""
                current_components = []
                reading_components = False
        continue
    
    # Ищем строку с "Наименование" - после неё начинаются компоненты
    if len(row) > 1 and row[1] == "Наименование":
        reading_components = True
        continue
    
    # Собираем компоненты
    if reading_components and current_kit:
        # Артикул компонента находится в колонке 2 (индекс 2)
        component_article = row[2] if len(row) > 2 else ""
        
        # Пропускаем пустые строки
        if component_article and component_article != "nan" and component_article != "":
            # Пропускаем служебные слова
            skip_words = ["гофроящик", "этикетка", "ложемент", "упаковка", "коробка", "бренд"]
            if not any(word in component_article.lower() for word in skip_words):
                if len(component_article) > 2:
                    current_components.append(component_article)
        
        # Если строка пустая - возможно, компоненты закончились
        if not component_article or component_article == "nan":
            # Проверяем, есть ли дальше еще компоненты
            if i + 1 < len(df_kits):
                next_row = df_kits.iloc[i + 1].astype(str).tolist()
                # Если следующая строка не содержит артикул, значит компоненты кончились
                if len(next_row) <= 2 or not next_row[2] or next_row[2] == "nan":
                    reading_components = False

# Сохраняем последний комплект
if current_kit and current_components:
    kits[current_kit] = {
        'name': current_kit_name,
        'components': list(set(current_components))
    }
    print(f"   ✅ {current_kit}: {len(set(current_components))} компонентов")

print(f"\n   ✅ ВСЕГО НАЙДЕНО КОМПЛЕКТОВ: {len(kits)}")

if len(kits) == 0:
    print("\n⚠️ КОМПЛЕКТЫ НЕ НАЙДЕНЫ!")
else:
    # Анализ наличия комплектов
    print("\n🔍 Анализ наличия на складе...")
    results = []
    
    for kit_article, kit_info in kits.items():
        print(f"   📦 {kit_article}: {kit_info['name'][:40]}")
        
        # Проверяем наличие каждого компонента
        min_available = float('inf')
        limiting_component = None
        
        for component in kit_info['components']:
            comp_norm = normalize_article(component)
            found = df_components[df_components['Код_норм'] == comp_norm]
            
            if found.empty:
                min_available = 0
                limiting_component = component
                break
            
            total_available = found['Наличие'].sum()
            if total_available < min_available:
                min_available = total_available
                limiting_component = component
        
        # Формируем вывод как в вашем примере
        # Строка с названием
        results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': '',
            'Цена': '',
            'Срок': ''
        })
        
        # Пустая строка
        results.append({
            'Комплект': '',
            'Артикул': '',
            'Бренд': '',
            'Количество': '',
            'Цена': '',
            'Срок': ''
        })
        
        # Срочная поставка (заглушка)
        results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': 0,
            'Цена': '—',
            'Срок': '—'
        })
        
        # Минимальная цена (заглушка)
        results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': 0,
            'Цена': '—',
            'Срок': '—'
        })
        
        # Реальный остаток
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
        
        # Две пустые строки между комплектами
        results.append({
            'Комплект': '',
            'Артикул': '',
            'Бренд': '',
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
    
    # Сохраняем результат
    output_file = f'results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    df_results = pd.DataFrame(results)
    df_results.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"\n✅ ГОТОВО!")
    print(f"📊 Результат сохранен: {output_file}")
    print(f"📈 Проанализировано комплектов: {len(kits)}")
    
    # Показываем результат
    print(f"\n📋 ПЕРВЫЕ 15 СТРОК РЕЗУЛЬТАТА:")
    print(df_results.head(15).to_string())

print("\n" + "=" * 60)
