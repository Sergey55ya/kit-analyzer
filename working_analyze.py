# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from collections import defaultdict
import os
import requests
import sys
import logging
from datetime import datetime
import json
import re

# ============================================
# 1. НАСТРОЙКИ ЛОГИРОВАНИЯ
# ============================================

log_filename = f'analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

# ============================================
# 2. НАСТРОЙКИ - ССЫЛКИ НА ФАЙЛЫ
# ============================================

# Файл с компонентами (уже в репозитории)
COMPONENTS_FILENAME = "components.xlsx"

# Файл с комплектами
KITS_URL = "https://docs.google.com/spreadsheets/d/1Cqr7WKBAaJqBjDBSU6flvouMtUKKO01Y/export?format=xlsx"
KITS_FILENAME = "kits.xlsx"

# ============================================
# 3. ФУНКЦИЯ НОРМАЛИЗАЦИИ
# ============================================

def normalize_article(article):
    """Нормализация артикула для поиска"""
    if pd.isna(article):
        return ""
    if not isinstance(article, str):
        article = str(article)
    result = article.replace(' ', '').replace('-', '')
    result = re.sub(r'[^A-Za-z0-9]', '', result)
    return result.upper()

# ============================================
# 4. ФУНКЦИЯ ПОИСКА АРТИКУЛОВ
# ============================================

def find_stock_items(article, df_stock):
    """Поиск артикулов с учётом разных форматов"""
    if df_stock.empty:
        return pd.DataFrame()
    
    original_article = str(article).strip()
    
    # Варианты для поиска
    variants = [original_article]
    
    # Удаление дефисов и пробелов
    no_spaces = original_article.replace(' ', '').replace('-', '')
    if no_spaces != original_article:
        variants.append(no_spaces)
    
    # Нормализация
    normalized = normalize_article(original_article)
    if normalized != original_article and normalized != no_spaces:
        variants.append(normalized)
    
    variants = list(dict.fromkeys(variants))
    
    # Поиск
    for variant in variants:
        result = df_stock[df_stock['Код'].astype(str) == variant]
        if not result.empty:
            return result
        
        result = df_stock[df_stock['Код'].astype(str).str.upper() == variant.upper()]
        if not result.empty:
            return result
    
    # Поиск по нормализации
    df_stock['Код_норм'] = df_stock['Код'].astype(str).apply(normalize_article)
    normalized_article = normalize_article(original_article)
    result = df_stock[df_stock['Код_норм'] == normalized_article]
    if not result.empty:
        return result
    
    return pd.DataFrame()

# ============================================
# 5. ЗАГРУЗКА ФАЙЛОВ
# ============================================

def download_file(url, filename):
    """Скачивает файл"""
    logger.info(f"📥 Скачиваю {filename}...")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            logger.info(f"   ✅ Скачан: {filename}")
            return True
    except Exception as e:
        logger.error(f"   ❌ Ошибка: {e}")
    return False

def load_components(filename):
    """Загружает файл с компонентами"""
    logger.info(f"📄 Загрузка компонентов из {filename}...")
    try:
        df = pd.read_excel(filename, sheet_name=0)
        
        # Переименовываем колонки
        column_mapping = {
            'Код': 'Код',
            'Бренд': 'Бренд', 
            'Название': 'Наименование',
            'ID поставщика': 'ID_поставщика',
            'Цена': 'Цена',
            'Наличие': 'Наличие',
            'Минимальный срок доставки': 'Срок'
        }
        
        existing_cols = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_cols)
        
        df['Код'] = df['Код'].astype(str).str.strip()
        df['Цена'] = pd.to_numeric(df['Цена'].astype(str).str.replace(',', '.'), errors='coerce')
        df['Наличие'] = pd.to_numeric(df['Наличие'], errors='coerce').fillna(0)
        df['Срок'] = pd.to_numeric(df['Срок'], errors='coerce').fillna(999)
        
        # Оставляем только товары с ценой
        df = df[df['Цена'] > 0]
        
        logger.info(f"   ✅ Загружено {len(df)} компонентов")
        
        # Показываем статистику по дублям артикулов
        duplicates = df.groupby('Код').size().sort_values(ascending=False)
        if len(duplicates) > 0:
            logger.info(f"   📊 Уникальных артикулов: {len(duplicates)}")
            logger.info(f"   📊 Максимум позиций на артикул: {duplicates.max()}")
        
        return df
    except Exception as e:
        logger.error(f"   ❌ Ошибка: {e}")
        return pd.DataFrame()

def clean_kit_name(full_name):
    """Очищает название комплекта"""
    if not isinstance(full_name, str):
        return full_name
    name = full_name.strip()
    if ' /' in name:
        return name.rsplit(' /', 1)[0].strip()
    if name.endswith('/'):
        return name[:-1].strip()
    return name

def parse_all_kits_from_file(filename):
    """Парсит файл со всеми комплектами"""
    logger.info(f"📋 Загрузка комплектов из {filename}...")
    
    try:
        df = pd.read_excel(filename, sheet_name=0, header=None)
        logger.info(f"   Всего строк в файле: {len(df)}")

        kits = {}
        current_kit = None
        kit_components = []
        kit_name = ""
        kit_article = ""

        i = 0
        while i < len(df):
            row = df.iloc[i].astype(str).tolist()
            
            if len(row) > 1 and row[1] == 'Комплект':
                if i + 1 < len(df):
                    next_row = df.iloc[i+1].astype(str).tolist()
                    if len(next_row) > 2:
                        potential_name = str(next_row[1]).strip()
                        potential_article = str(next_row[2]).strip()
                        
                        if (potential_name and potential_name != 'nan' and
                            potential_article and potential_article != 'nan' and
                            len(potential_article) > 3):
                            
                            if current_kit and len(kit_components) > 0:
                                unique_components = []
                                seen = set()
                                for comp in kit_components:
                                    if comp not in seen and comp not in ['nan', 'Артикул']:
                                        seen.add(comp)
                                        unique_components.append(comp)
                                
                                if len(unique_components) > 0:
                                    clean_name = clean_kit_name(kit_name)
                                    kits[kit_article] = {
                                        'name': clean_name,
                                        'components': unique_components
                                    }
                                    logger.info(f"      ✅ Загружен комплект {kit_article}: {len(unique_components)} компонентов")
                            
                            kit_name = potential_name
                            kit_article = potential_article
                            kit_components = []
                            current_kit = kit_article
                            i += 2
                            continue
            
            if current_kit and len(row) > 2:
                article = str(row[2]).strip()
                if (article and article != 'nan' and article != 'Артикул' and
                    not article.startswith('УТ') and len(article) > 1 and len(article) < 30):
                    exclude_words = ['гофроящик', 'этикетка', 'ложемент', 'наименование',
                                   'комплект', 'бренд', 'код', 'упаковка', 'коробка']
                    article_lower = article.lower()
                    if not any(word in article_lower for word in exclude_words):
                        kit_components.append(article)
            
            i += 1

        if current_kit and len(kit_components) > 0:
            unique_components = []
            seen = set()
            for comp in kit_components:
                if comp not in seen and comp not in ['nan', 'Артикул']:
                    seen.add(comp)
                    unique_components.append(comp)
            
            if len(unique_components) > 0:
                clean_name = clean_kit_name(kit_name)
                kits[kit_article] = {
                    'name': clean_name,
                    'components': unique_components
                }
                logger.info(f"      ✅ Загружен комплект {kit_article}: {len(unique_components)} компонентов")

        logger.info(f"\n   ✅ Всего загружено комплектов: {len(kits)}")
        return kits
    except Exception as e:
        logger.error(f"   ❌ Ошибка: {e}")
        return {}

# ============================================
# 6. ОСНОВНАЯ ФУНКЦИЯ АНАЛИЗА (ИСПРАВЛЕННАЯ)
# ============================================

def calculate_max_quantity_with_groups(components, df_stock, kit_article):
    """
    Рассчитать максимальное количество комплектов
    Учитывает ВСЕ позиции с разными поставщиками, ценами и сроками
    """
    if df_stock.empty:
        return 0, [], None, None

    available_items = {}
    missing_articles = []

    logger.info(f"      Поиск компонентов для {kit_article} (всего {len(components)}):")
    
    for article in components:
        items = find_stock_items(article, df_stock)

        if items.empty:
            missing_articles.append(article)
            continue

        # Берем все позиции с ценой
        available = items[pd.notna(items['Цена']) & (items['Цена'] > 0)].copy()
        
        if available.empty:
            missing_articles.append(article)
            continue

        # Сортируем для оптимального выбора
        available = available.sort_values(['Срок', 'Цена'])
        
        # Сохраняем ВСЕ позиции
        available_items[article] = []
        for _, row in available.iterrows():
            available_items[article].append({
                'source': str(row.get('ID_поставщика', '?')),
                'price': float(row['Цена']),
                'delivery': int(row['Срок']) if not pd.isna(row['Срок']) else 999,
                'qty': int(row['Наличие']) if not pd.isna(row['Наличие']) else 0
            })
        
        logger.info(f"      📦 {article}: найдено {len(available_items[article])} позиций")

    if missing_articles:
        logger.warning(f"      ⚠️ Отсутствуют {len(missing_articles)} компонентов")
        return 0, [], missing_articles[0] if missing_articles else None, 0

    # Создаем копии для сборки
    stock_copies = {}
    for article, items in available_items.items():
        stock_copies[article] = [item.copy() for item in items]
    
    # Теоретический максимум
    min_total_qty = min(sum(item['qty'] for item in items) for items in available_items.values())
    logger.info(f"      Теоретический максимум: {min_total_qty} комплектов")
    
    # Собираем комплекты
    kits_assembled = []
    
    for kit_num in range(min_total_qty):
        kit_price = 0
        kit_delivery = 0
        kit_complete = True
        
        for article in components:
            if article not in stock_copies:
                kit_complete = False
                break
            
            # Выбираем лучшую доступную позицию
            best_idx = -1
            best_price = float('inf')
            best_delivery = float('inf')
            
            for i, source in enumerate(stock_copies[article]):
                if source['qty'] > 0:
                    if (source['delivery'] < best_delivery or 
                        (source['delivery'] == best_delivery and source['price'] < best_price)):
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
    
    # Группировка результатов
    grouped = defaultdict(int)
    for kit in kits_assembled:
        key = (kit['price'], kit['delivery'])
        grouped[key] += 1
    
    result_groups = []
    for (price, delivery), count in sorted(grouped.items()):
        result_groups.append({
            'count': count,
            'price': price,
            'delivery': delivery
        })
    
    total_kits = len(kits_assembled)
    logger.info(f"      ✅ Собрано комплектов: {total_kits}")
    
    # Лимитирующий компонент
    limiting_article = None
    limiting_qty = min_total_qty
    for article in components:
        remaining = sum(item['qty'] for item in stock_copies[article])
        used = sum(item['qty'] for item in available_items[article]) - remaining
        if used < limiting_qty:
            limiting_qty = used
            limiting_article = article
    
    return total_kits, result_groups, limiting_article, limiting_qty

# ============================================
# 7. ОСНОВНАЯ ФУНКЦИЯ
# ============================================

def main():
    logger.info("="*70)
    logger.info("🚀 ЗАПУСК АНАЛИЗА СКЛАДСКИХ ОСТАТКОВ")
    logger.info(f"📅 Дата запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*70)
    
    # Загрузка компонентов
    if not os.path.exists(COMPONENTS_FILENAME):
        logger.error(f"❌ Файл {COMPONENTS_FILENAME} не найден в репозитории!")
        logger.info("📌 Загрузите файл components.xlsx в репозиторий")
        return
    
    df_stock = load_components(COMPONENTS_FILENAME)
    if df_stock.empty:
        logger.error("❌ Не удалось загрузить компоненты")
        return
    
    # Загрузка комплектов
    if not download_file(KITS_URL, KITS_FILENAME):
        logger.error("❌ Не удалось скачать файл с комплектами")
        return
    
    kits = parse_all_kits_from_file(KITS_FILENAME)
    if not kits:
        logger.error("❌ Нет загруженных комплектов для анализа!")
        return
    
    # Анализ
    logger.info("\n🔍 АНАЛИЗ КОМПЛЕКТОВ")
    logger.info("="*70)
    
    all_results = []
    
    for kit_article, kit_info in kits.items():
        logger.info(f"\n▶️ Анализ {kit_article} ({kit_info['name']})...")
        
        max_qty, groups, limiting_art, limiting_qty = calculate_max_quantity_with_groups(
            kit_info['components'], df_stock, kit_article
        )
        
        # Заголовок комплекта
        all_results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': '',
            'Цена': '',
            'Срок': ''
        })
        all_results.append({
            'Комплект': '',
            'Артикул': '',
            'Бренд': '',
            'Количество': '',
            'Цена': '',
            'Срок': ''
        })
        
        # Срочная поставка
        all_results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': 0,
            'Цена': '—',
            'Срок': '—'
        })
        
        # Минимальная цена
        all_results.append({
            'Комплект': kit_info['name'],
            'Артикул': kit_article,
            'Бренд': 'PowerMechanics',
            'Количество': 0,
            'Цена': '—',
            'Срок': '—'
        })
        
        # Результаты
        if max_qty > 0 and groups:
            for group in groups:
                all_results.append({
                    'Комплект': kit_info['name'],
                    'Артикул': kit_article,
                    'Бренд': 'PowerMechanics',
                    'Количество': group['count'],
                    'Цена': f"{group['price']:.2f} ₽",
                    'Срок': str(group['delivery'])
                })
            
            all_results.append({
                'Комплект': 'Всего комплектов по наличию:',
                'Артикул': '',
                'Бренд': '',
                'Количество': max_qty,
                'Цена': '',
                'Срок': ''
            })
            if limiting_art:
                all_results.append({
                    'Комплект': 'Лимитирующий компонент:',
                    'Артикул': limiting_art,
                    'Бренд': '',
                    'Количество': limiting_qty,
                    'Цена': '',
                    'Срок': ''
                })
        else:
            all_results.append({
                'Комплект': kit_info['name'],
                'Артикул': kit_article,
                'Бренд': 'PowerMechanics',
                'Количество': 0,
                'Цена': '—',
                'Срок': '—'
            })
        
        all_results.append({
            'Комплект': '',
            'Артикул': '',
            'Бренд': '',
            'Количество': '',
            'Цена': '',
            'Срок': ''
        })
    
    # Сохранение
    output_filename = f'results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    df_results = pd.DataFrame(all_results)
    df_results.to_csv(output_filename, index=False, encoding='utf-8-sig')
    
    logger.info(f"\n💾 Результаты сохранены в файл: {output_filename}")
    logger.info(f"📊 Проанализировано комплектов: {len(kits)}")
    logger.info("✨ Анализ завершен успешно!")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)
