# -*- coding: utf-8 -*-
import pandas as pd
import requests
from datetime import datetime
import os
import sys
import re
from collections import defaultdict

print("="*70)
print("🚀 ЗАПУСК АНАЛИЗА (С ЛИМИТИРУЮЩИМ КОМПОНЕНТОМ)")
print("="*70)

# ============================================
# НАСТРОЙКИ
# ============================================

COMPONENTS_URL = "https://admin.silam.ru/system/unload_prices/29/zzap_copy_1.xlsx?rand=c5149f53-bd66-469f-880f-534e624f837a"
COMPONENTS_FILENAME = "components.xlsx"

KITS_URL = "https://docs.google.com/spreadsheets/d/1Cqr7WKBAaJqBjDBSU6flvouMtUKKO01Y/export?format=xlsx"
KITS_FILENAME = "kits.xlsx"

# ============================================
# ФУНКЦИИ
# ============================================

def download_file(url, filename):
    print(f"📥 Скачиваю {filename}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=60)
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

def normalize_article(article):
    if pd.isna(article):
        return ""
    article = str(article).strip()
    article = article.replace('-', '').replace(' ', '')
    article = re.sub(r'[^A-Za-z0-9]', '', article)
    return article.upper()

def load_components(filename):
    print(f"\n📄 Загрузка компонентов...")
    try:
        df = pd.read_excel(filename, sheet_name=0)
        
        column_mapping = {
            'Код': 'Код', 'Бренд': 'Бренд', 'Название': 'Наименование',
            'ID поставщика': 'ID_поставщика', 'Цена': 'Цена',
            'Наличие': 'Наличие', 'Минимальный срок доставки': 'Срок'
        }
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_cols)
        
        if 'Код' in df.columns:
            df['Код'] = df['Код'].astype(str).str.strip()
            df['Код_норм'] = df['Код'].apply(normalize_article)
        else:
            print("   ❌ Нет колонки 'Код'")
            return pd.DataFrame()
        
        if 'Цена' in df.columns:
            df['Цена'] = df['Цена'].astype(str).str.replace(',', '.').str.replace(' ', '')
            df['Цена'] = pd.to_numeric(df['Цена'], errors='coerce').fillna(0)
        else:
            df['Цена'] = 0
            
        if 'Наличие' in df.columns:
            df['Наличие'] = pd.to_numeric(df['Наличие'], errors='coerce').fillna(0)
        else:
            df['Наличие'] = 0
            
        if 'Срок' in df.columns:
            df['Срок'] = pd.to_numeric(df['Срок'], errors='coerce').fillna(999)
        else:
            df['Срок'] = 999
            
        if 'ID_поставщика' not in df.columns:
            df['ID_поставщика'] = 'неизвестно'
        
        df = df[df['Цена'] > 0]
        
        print(f"   ✅ Загружено {len(df)} компонентов")
        print(f"   📊 Уникальных артикулов: {df['Код'].nunique()}")
        
        return df
    except Exception as e:
        print(f"   ❌ Ошибка загрузки: {e}")
        return pd.DataFrame()

def parse_kits(filename):
    print(f"\n📋 Загрузка комплектов...")
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
            
            if len(row) > 1 and row[1] == "Наименование":
                reading = True
                continue
            
            if reading and current_kit:
                art = row[2] if len(row) > 2 else ""
                if art and art != "nan" and len(art) > 2:
                    exclude = ['гофроящик', 'этикетка', 'ложемент', 'упаковка', 'коробка']
                    if not any(w in art.lower() for w in exclude):
                        current_comps.append(art)
        
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

def calculate_kits_with_price_and_delivery(components, df_stock):
    """Рассчитывает комплекты с ценами, сроками и лимитирующим компонентом"""
    if df_stock.empty:
        return 0, [], None, 0
    
    # Собираем все доступные позиции для каждого компонента
    available_items = {}
    
    for article in components:
        comp_norm = normalize_article(article)
        found = df_stock[df_stock['Код_норм'] == comp_norm]
        
        if found.empty:
            return 0, [], article, 0  # Лимитирующий компонент - тот, которого нет
        
        # Сортируем по сроку и цене
        found = found.sort_values(['Срок', 'Цена'])
        
        available_items[article] = []
        for _, row in found.iterrows():
            available_items[article].append({
                'price': float(row['Цена']),
                'delivery': int(row['Срок']) if not pd.isna(row['Срок']) else 999,
                'qty': int(row['Наличие']) if not pd.isna(row['Наличие']) else 0
            })
    
    # Находим максимальное количество комплектов и лимитирующий компонент
    max_kits = float('inf')
    limiting_article = None
    limiting_qty = 0
    
    for article, items in available_items.items():
        total_qty = sum(item['qty'] for item in items)
        if total_qty < max_kits:
            max_kits = total_qty
            limiting_article = article
            limiting_qty = total_qty
    
    if max_kits == float('inf') or max_kits == 0:
        return 0, [], limiting_article, limiting_qty
    
    max_kits = int(max_kits)
    
    # Создаем копии остатков
    stock_copies = {}
    for article, items in available_items.items():
        stock_copies[article] = [item.copy() for item in items]
    
    # Собираем комплекты
    kits_assembled = []
    
    for _ in range(max_kits):
        kit_price = 0
        kit_delivery = 0
        kit_complete = True
        
        for article in components:
            if article not in stock_copies:
                kit_complete = False
                break
            
            best_idx = -1
            best_delivery = float('inf')
            best_price = float('inf')
            
            for i, source in enumerate(stock_copies[article]):
                if source['qty'] > 0:
                    if source['delivery'] < best_delivery or (
                        source['delivery'] == best_delivery and source['price'] < best_price):
                        best_delivery = source['delivery']
                        best_price = source['price']
                        best_idx = i
            
            if best_idx >= 0:
                source = stock_copies[article][best_idx]
                kit_price += source['price']
                if source['delivery'] > kit_delivery:
                    kit_delivery = source['delivery']
                source['qty'] -= 1
            else:
                kit_complete = False
                break
        
        if kit_complete:
            kits_assembled.append({
                'price': round(kit_price, 2),
                'delivery': kit_delivery
            })
    
    # Группируем одинаковые комплекты
    grouped = defaultdict(int)
    for kit in kits_assembled:
        key = (kit['price'], kit['delivery'])
        grouped[key] += 1
    
    result = []
    for (price, delivery), count in sorted(grouped.items()):
        result.append({
            'count': count,
            'price': price,
            'delivery': delivery
        })
    
    return len(kits_assembled), result, limiting_article, limiting_qty

# ============================================
# ОСНОВНАЯ ЛОГИКА
# ============================================

def main():
    # 1. Скачиваем компоненты
    if not download_file(COMPONENTS_URL, COMPONENTS_FILENAME):
        print("❌ Не удалось скачать компоненты")
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
        
        max_qty, groups, limiting_art, limiting_qty = calculate_kits_with_price_and_delivery(
            info['components'], df_stock
        )
        
        print(f"   Можно собрать: {max_qty} шт.")
        if groups:
            for g in groups:
                print(f"      {g['count']} шт. по {g['price']:.2f} ₽, срок {g['delivery']} дн.")
        if limiting_art:
            print(f"   Лимитирующий компонент: {limiting_art} ({limiting_qty} шт.)")
        
        # Заголовок комплекта
        results.append({'Комплект': info['name'], 'Артикул': art, 'Бренд': 'PowerMechanics', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
        
        # Срочная поставка (заглушка)
        results.append({'Комплект': info['name'], 'Артикул': art, 'Бренд': 'PowerMechanics', 'Количество': 0, 'Цена': '—', 'Срок': '—'})
        
        # Минимальная цена (заглушка)
        results.append({'Комплект': info['name'], 'Артикул': art, 'Бренд': 'PowerMechanics', 'Количество': 0, 'Цена': '—', 'Срок': '—'})
        
        # Реальные результаты
        if max_qty > 0 and groups:
            for group in groups:
                results.append({
                    'Комплект': info['name'],
                    'Артикул': art,
                    'Бренд': 'PowerMechanics',
                    'Количество': group['count'],
                    'Цена': f"{group['price']:.2f} ₽",
                    'Срок': str(group['delivery'])
                })
            
            results.append({
                'Комплект': 'Всего комплектов по наличию:',
                'Артикул': '',
                'Бренд': '',
                'Количество': max_qty,
                'Цена': '',
                'Срок': ''
            })
            
            # Добавляем лимитирующий компонент
            if limiting_art:
                results.append({
                    'Комплект': 'Лимитирующий компонент:',
                    'Артикул': limiting_art,
                    'Бренд': '',
                    'Количество': limiting_qty,
                    'Цена': '',
                    'Срок': ''
                })
        else:
            results.append({
                'Комплект': info['name'],
                'Артикул': art,
                'Бренд': 'PowerMechanics',
                'Количество': 0,
                'Цена': '—',
                'Срок': '—'
            })
            
            # Если нет в наличии, показываем первый отсутствующий компонент
            if limiting_art:
                results.append({
                    'Комплект': 'Отсутствует компонент:',
                    'Артикул': limiting_art,
                    'Бренд': '',
                    'Количество': 0,
                    'Цена': '',
                    'Срок': ''
                })
        
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
        results.append({'Комплект': '', 'Артикул': '', 'Бренд': '', 'Количество': '', 'Цена': '', 'Срок': ''})
    
    # Сохраняем результат
    output_file = f'results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    df_results = pd.DataFrame(results)
    df_results.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print("\n" + "="*70)
    print(f"✅ ГОТОВО!")
    print(f"📊 Файл: {output_file}")
    print(f"📈 Строк: {len(df_results)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
