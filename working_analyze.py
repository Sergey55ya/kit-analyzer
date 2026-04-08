# -*- coding: utf-8 -*-
import pandas as pd
import requests
from datetime import datetime
import os
import sys
import re

print("="*70)
print("🚀 ЗАПУСК АНАЛИЗА (УПРОЩЕННАЯ ВЕРСИЯ)")
print("="*70)

# ============================================
# НАСТРОЙКИ
# ============================================

COMPONENTS_FILENAME = "components.xlsx"
KITS_URL = "https://docs.google.com/spreadsheets/d/1Cqr7WKBAaJqBjDBSU6flvouMtUKKO01Y/export?format=xlsx"
KITS_FILENAME = "kits.xlsx"

# ============================================
# ФУНКЦИИ
# ============================================

def normalize_article(article):
    if pd.isna(article):
        return ""
    article = str(article).strip()
    article = article.replace('-', '').replace(' ', '')
    article = re.sub(r'[^A-Za-z0-9]', '', article)
    return article.upper()

def download_file(url, filename):
    print(f"📥 Скачиваю {filename}...")
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            print(f"   ✅ Скачан: {filename} ({len(response.content)} байт)")
            return True
        else:
            print(f"   ❌ HTTP {response.status_code}")
            return False
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
        return False

def load_components(filename):
    print(f"\n📄 Загрузка компонентов из {filename}...")
    try:
        df = pd.read_excel(filename, sheet_name=0)
        print(f"   Колонки: {df.columns.tolist()}")
        
        # Нормализуем артикулы
        if 'Код' in df.columns:
            df['Код'] = df['Код'].astype(str).str.strip()
            df['Код_норм'] = df['Код'].apply(normalize_article)
        else:
            print("   ❌ Нет колонки 'Код'")
            return pd.DataFrame()
        
        # Конвертируем наличие
        if 'Наличие' in df.columns:
            df['Наличие'] = pd.to_numeric(df['Наличие'], errors='coerce').fillna(0)
        else:
            df['Наличие'] = 0
            
        if 'Цена' in df.columns:
            df['Цена'] = pd.to_numeric(df['Цена'].astype(str).str.replace(',', '.'), errors='coerce').fillna(0)
        else:
            df['Цена'] = 0
            
        print(f"   ✅ Загружено {len(df)} компонентов")
        print(f"   📊 Уникальных артикулов: {df['Код'].nunique()}")
        
        return df
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
        return pd.DataFrame()

def parse_kits(filename):
    print(f"\n📋 Загрузка комплектов из {filename}...")
    try:
        df = pd.read_excel(filename, sheet_name=0, header=None)
        print(f"   Всего строк: {len(df)}")
        
        kits = {}
        current_kit = None
        current_name = ""
        current_comps = []
        reading = False
        
        for i in range(len(df)):
            row = df.iloc[i].astype(str).tolist()
            while len(row) < 7:
                row.append("")
            
            # Ищем комплект
            if len(row) > 1 and row[1] == "Комплект":
                if current_kit and current_comps:
                    kits[current_kit] = {
                        'name': current_name,
                        'components': list(set(current_comps))
                    }
                    print(f"   ✅ {current_kit}: {len(set(current_comps))} комп.")
                
                if i + 1 < len(df):
                    next_row = df.iloc[i+1].astype(str).tolist()
                    if len(next_row) > 2:
                        current_name = next_row[1] if next_row[1] != "nan" else ""
                        current_kit = next_row[2] if next_row[2] != "nan" else ""
                        current_comps = []
                        reading = False
                continue
            
            # Начало списка компонентов
            if len(row) > 1 and row[1] == "Наименование":
                reading = True
                continue
            
            # Собираем компоненты
            if reading and current_kit:
                art = row[2] if len(row) > 2 else ""
                if art and art != "nan" and len(art) > 2:
                    exclude = ['гофроящик', 'этикетка', 'ложемент', 'упаковка', 'коробка']
                    if not any(w in art.lower() for w in exclude):
                        current_comps.append(art)
        
        # Последний комплект
        if current_kit and current_comps:
            kits[current_kit] = {
                'name': current_name,
                'components': list(set(current_comps))
            }
            print(f"   ✅ {current_kit}: {len(set(current_comps))} комп.")
        
        print(f"\n   ✅ ВСЕГО КОМПЛЕКТОВ: {len(kits)}")
        return kits
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
        return {}

# ============================================
# ОСНОВНАЯ ЛОГИКА
# ============================================

def main():
    # 1. Проверяем компоненты
    if not os.path.exists(COMPONENTS_FILENAME):
        print(f"\n❌ Файл {COMPONENTS_FILENAME} не найден в репозитории!")
        print("📌 Загрузите файл components.xlsx в репозиторий")
        
        # Создаем пустой результат
        df_result = pd.DataFrame({
            'Комплект': ['ОШИБКА'],
            'Артикул': ['Файл компонентов не найден'],
            'Бренд': [''],
            'Количество': [''],
            'Цена': [''],
            'Срок': ['']
        })
        output_file = f'results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        df_result.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n💾 Создан файл с ошибкой: {output_file}")
        return
    
    # 2. Загружаем компоненты
    df_stock = load_components(COMPONENTS_FILENAME)
    if df_stock.empty:
        print("❌ Не удалось загрузить компоненты")
        return
    
    # 3. Скачиваем комплекты
    if not download_file(KITS_URL, KITS_FILENAME):
        print("❌ Не удалось скачать комплекты")
        return
    
    # 4. Парсим комплекты
    kits = parse_kits(KITS_FILENAME)
    if not kits:
        print("❌ Комплекты не найдены")
        return
    
    # 5. Анализируем
    print("\n🔍 АНАЛИЗ КОМПЛЕКТОВ")
    print("="*70)
    
    results = []
    
    for art, info in kits.items():
        print(f"\n📦 {art}: {info['name'][:40]}")
        
        # Ищем минимальное количество
        min_qty = float('inf')
        for comp in info['components']:
            comp_norm = normalize_article(comp)
            found = df_stock[df_stock['Код_норм'] == comp_norm]
            if found.empty:
                min_qty = 0
                break
            total = found['Наличие'].sum()
            if total < min_qty:
                min_qty = total
        
        if min_qty == float('inf'):
            min_qty = 0
        
        print(f"   Можно собрать: {min_qty} шт.")
        
        # Формируем вывод
        results.append({'Комплект': info['name'], 'Артикул': art, 'Бренд': 'PowerMechanics', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': info['name'], 'Артикул': art, 'Бренд': 'PowerMechanics', 'Количество': 0, 'Цена': '—', 'Срок': '—'})
        results.append({'Комплект': info['name'], 'Артикул': art, 'Бренд': 'PowerMechanics', 'Количество': 0, 'Цена': '—', 'Срок': '—'})
        results.append({'Комплект': info['name'], 'Артикул': art, 'Бренд': 'PowerMechanics', 'Количество': int(min_qty) if min_qty > 0 else 0, 'Цена': '—', 'Срок': '—'})
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
    
    # СОХРАНЯЕМ РЕЗУЛЬТАТ
    output_file = f'results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    df_results = pd.DataFrame(results)
    df_results.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print("\n" + "="*70)
    print(f"✅ ГОТОВО!")
    print(f"📊 Файл: {output_file}")
    print(f"📈 Строк: {len(df_results)}")
    
    # Проверяем, что файл создан
    if os.path.exists(output_file):
        print(f"✅ Файл подтвержден! Размер: {os.path.getsize(output_file)} байт")
    else:
        print(f"❌ ФАЙЛ НЕ СОЗДАН!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        
        # Создаем файл с ошибкой
        df_err = pd.DataFrame({'Ошибка': [str(e)]})
        df_err.to_csv(f'error_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv', index=False)
        print("Создан файл с ошибкой")
