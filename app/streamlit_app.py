"""
Streamlit приложение для визуализации климатических данных
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
import numpy as np
import datetime
from datetime import datetime, timedelta
from app.database import init_db, load_data_from_db, get_aggregated_data, save_dataframe_to_db, clear_table

# Настройка страницы
st.set_page_config(
    page_title="World Weather Analytics",
    page_icon="🌍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Глобальные настройки
DATA_REFRESH_INTERVAL = 300  # 5 минут для автоматического обновления данных

# Инициализация БД с обработкой ошибок
try:
    init_db()
except Exception as e:
    st.error(f"Ошибка инициализации БД: {str(e)}")

# Координаты всех городов для карты
CITY_COORDINATES = {
    # Европа
    "London": {"lat": 51.50, "lon": -0.12},
    "Berlin": {"lat": 52.52, "lon": 13.41},
    "Paris": {"lat": 48.85, "lon": 2.35},
    "Madrid": {"lat": 40.41, "lon": -3.70},
    "Moscow": {"lat": 55.75, "lon": 37.61},
    "Rome": {"lat": 41.90, "lon": 12.49},
    "Stockholm": {"lat": 59.32, "lon": 18.06},
    "Athens": {"lat": 37.98, "lon": 23.72},
    "Vienna": {"lat": 48.20, "lon": 16.37},
    "Amsterdam": {"lat": 52.37, "lon": 4.90},
    "Prague": {"lat": 50.07, "lon": 14.43},
    "Warsaw": {"lat": 52.22, "lon": 21.01},
    "Oslo": {"lat": 59.91, "lon": 10.75},
    "Helsinki": {"lat": 60.17, "lon": 24.94},
    
    # Северная Америка
    "New York": {"lat": 40.71, "lon": -74.01},
    "Los Angeles": {"lat": 34.05, "lon": -118.24},
    "Chicago": {"lat": 41.87, "lon": -87.62},
    "Toronto": {"lat": 43.65, "lon": -79.38},
    "Mexico City": {"lat": 19.43, "lon": -99.13},
    "Vancouver": {"lat": 49.28, "lon": -123.12},
    "Miami": {"lat": 25.76, "lon": -80.19},
    "San Francisco": {"lat": 37.77, "lon": -122.41},
    "Boston": {"lat": 42.36, "lon": -71.06},
    "Denver": {"lat": 39.74, "lon": -104.99},
    
    # Азия и Тихий океан
    "Tokyo": {"lat": 35.68, "lon": 139.69},
    "Singapore": {"lat": 1.35, "lon": 103.82},
    "Mumbai": {"lat": 19.07, "lon": 72.87},
    "Sydney": {"lat": -33.87, "lon": 151.21},
    "Seoul": {"lat": 37.56, "lon": 126.97},
    "Beijing": {"lat": 39.90, "lon": 116.40},
    "Shanghai": {"lat": 31.23, "lon": 121.47},
    "Bangkok": {"lat": 13.75, "lon": 100.50},
    "Kuala Lumpur": {"lat": 3.13, "lon": 101.68},
    "Auckland": {"lat": -36.85, "lon": 174.76},
    
    # Южная Америка и Африка
    "Rio de Janeiro": {"lat": -22.90, "lon": -43.17},
    "Sao Paulo": {"lat": -23.55, "lon": -46.63},
    "Buenos Aires": {"lat": -34.60, "lon": -58.38},
    "Lima": {"lat": -12.04, "lon": -77.04},
    "Cairo": {"lat": 30.04, "lon": 31.23},
    "Johannesburg": {"lat": -26.20, "lon": 28.04},
    "Cape Town": {"lat": -33.92, "lon": 18.42},
    "Nairobi": {"lat": -1.29, "lon": 36.82},
    "Lagos": {"lat": 6.52, "lon": 3.38}
}

# Кэширование функции получения агрегированных данных
@st.cache_data(ttl=DATA_REFRESH_INTERVAL)
def cached_get_aggregated_data():
    """Кэшированная функция для получения агрегированных данных"""
    try:
        return get_aggregated_data()
    except Exception as e:
        st.error(f"Ошибка загрузки агрегированных данных: {str(e)}")
        return pd.DataFrame()

# Кэширование доступных городов из базы
@st.cache_data(ttl=DATA_REFRESH_INTERVAL)
def get_available_cities():
    """Возвращает список городов, которые реально есть в БД"""
    try:
        # Сначала смотрим агрегаты (самый быстрый способ)
        from app.database import engine
        df = pd.read_sql("SELECT DISTINCT city FROM weather_aggregated ORDER BY city", engine)
        cities = df["city"].tolist()
        if cities:
            return cities

        # Фоллбек: берем уникальные города из raw-данных
        df = pd.read_sql("SELECT DISTINCT city FROM weather_data ORDER BY city", engine)
        return df["city"].tolist()
    except Exception as e:
        st.error(f"Ошибка получения списка городов: {str(e)}")
        return []

@st.cache_data(ttl=DATA_REFRESH_INTERVAL)
def get_data_range():
    """Минимальная и максимальная дата в таблице weather_data"""
    try:
        from app.database import engine
        df = pd.read_sql("SELECT MIN(time) AS min_time, MAX(time) AS max_time FROM weather_data", engine)
        if df.empty or pd.isnull(df.loc[0, "min_time"]):
            return None, None
        return pd.to_datetime(df.loc[0, "min_time"]), pd.to_datetime(df.loc[0, "max_time"])
    except Exception as e:
        st.error(f"Ошибка получения диапазона дат: {str(e)}")
        return None, None

# Кэширование функции загрузки данных
@st.cache_data(ttl=DATA_REFRESH_INTERVAL)
def cached_load_data(city=None, start_date=None, end_date=None, limit=10000):
    """Кэшированная функция для загрузки данных с фильтрами"""
    try:
        # Если указаны даты, используем SQL запрос с фильтрацией
        if start_date and end_date:
            query = f"""
            SELECT * FROM weather_data 
            WHERE time BETWEEN '{start_date}' AND '{end_date}'
            """
            if city and city != "All":
                query += f" AND city = '{city}'"
            query += f" ORDER BY time DESC LIMIT {limit}"
            
            # Используем engine напрямую для выполнения запроса
            from app.database import engine
            df = pd.read_sql(query, engine)
        else:
            df = load_data_from_db(city=city, limit=limit)
        
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки данных: {str(e)}")
        return pd.DataFrame()

# Функция для получения данных за последний день для всех городов
def get_last_24h_data():
    """Получение данных за последние 24 часа для всех городов"""
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)
    
    try:
        from app.database import engine
        query = f"""
        SELECT * FROM weather_data 
        WHERE time BETWEEN '{start_time.strftime("%Y-%m-%d %H:%M:%S")}' 
        AND '{end_time.strftime("%Y-%m-%d %H:%M:%S")}'
        ORDER BY time DESC
        """
        return pd.read_sql(query, engine)
    except Exception as e:
        st.error(f"Ошибка загрузки последних данных: {str(e)}")
        return pd.DataFrame()

# Заголовок
st.title("🌍 World Weather Analytics Dashboard")
st.markdown("---")

# Боковая панель с фильтрами
st.sidebar.header("📊 Фильтры и настройки")

# Выбор города
available_cities = get_available_cities()
all_cities = ["All"] + available_cities
selected_city = st.sidebar.selectbox("Выберите город", all_cities, index=0)

# Фильтры по времени
st.sidebar.subheader("⏰ Диапазон времени")
time_filter_type = st.sidebar.radio("Тип фильтра", ["Произвольный период"])

if time_filter_type == "Последние данные":
    time_periods = {
        "Последние 24 часа": 1,
        "Последние 7 дней": 7,
        "Последние 30 дней": 30,
        "Последние 90 дней": 90
    }
    selected_period = st.sidebar.selectbox("Временной период", list(time_periods.keys()))
    days_back = time_periods[selected_period]
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days_back)
else:
    data_min, data_max = get_data_range()
    default_start = data_min.date() if data_min is not None else (datetime.now() - timedelta(days=30)).date()
    default_end = data_max.date() if data_max is not None else datetime.now().date()
    start_date = st.sidebar.date_input(
        "Начальная дата",
        default_start,
        min_value=data_min.date() if data_min is not None else None,
        max_value=data_max.date() if data_max is not None else None,
    )
    end_date = st.sidebar.date_input(
        "Конечная дата",
        default_end,
        min_value=data_min.date() if data_min is not None else None,
        max_value=data_max.date() if data_max is not None else None,
    )

# Количество записей для отображения
limit = st.sidebar.slider("Максимум записей", 100, 50000, 5000, 100)

# Выбор метрик для отображения
st.sidebar.subheader("📈 Метрики для анализа")
available_metrics = [
    "temperature", "apparent_temperature", "dewpoint_2m", "humidity",
    "precipitation", "rain", "showers", "snowfall",
    "wind_speed", "wind_gusts_10m", "pressure", 
    "cloud_cover", "shortwave_radiation", "uv_index",
    "sunshine_duration"
]
metric_labels = {
    "temperature": "Температура",
    "apparent_temperature": "Ощущаемая температура",
    "dewpoint_2m": "Точка росы",
    "humidity": "Влажность",
    "precipitation": "Осадки",
    "rain": "Дождь",
    "showers": "Ливни",
    "snowfall": "Снегопад",
    "wind_speed": "Скорость ветра",
    "wind_gusts_10m": "Порывы ветра",
    "pressure": "Давление",
    "cloud_cover": "Облачность",
    "shortwave_radiation": "Солнечная радиация",
    "uv_index": "UV индекс",
    "sunshine_duration": "Солнечное сияние",
}
metric_units = {
    "temperature": "°C",
    "apparent_temperature": "°C",
    "dewpoint_2m": "°C",
    "humidity": "%",
    "precipitation": "мм",
    "rain": "мм",
    "showers": "мм",
    "snowfall": "мм",
    "wind_speed": "м/с",
    "wind_gusts_10m": "м/с",
    "pressure": "гПа",
    "cloud_cover": "%",
    "shortwave_radiation": "Вт/м²",
    "uv_index": "",
    "sunshine_duration": "с",
}
selected_metrics = st.sidebar.multiselect(
    "Выберите метрики",
    available_metrics,
    default=["temperature", "humidity"]
)

# Основной контент
try:
    # Загрузка данных
    df = cached_load_data(
        city=None if selected_city == "All" else selected_city,
        start_date=start_date.strftime("%Y-%m-%d") if isinstance(start_date, datetime) else start_date,
        end_date=end_date.strftime("%Y-%m-%d") if isinstance(end_date, datetime) else end_date,
        limit=limit
    )
    
    if df.empty:
        st.warning("⚠️ Нет данных в базе. Запустите ETL процесс через API или Prefect.")
        st.info("💡 Используйте `/etl/ingest` endpoint для загрузки данных")
    else:
        # Преобразование времени
        df['time'] = pd.to_datetime(df['time'])
        selected_metrics = [m for m in selected_metrics if m in df.columns]
        
        # Метрики
        st.subheader("📈 Ключевые показатели")
        
        metric_cols = st.columns(len(selected_metrics) + 2)
        
        # Город и период
        with metric_cols[0]:
            st.metric("🏙️ Город", selected_city if selected_city != "All" else "Все города")
        
        with metric_cols[1]:
            st.metric("⏱️ Период", f"{start_date.strftime('%d.%m.%Y') if hasattr(start_date, 'strftime') else start_date} - {end_date.strftime('%d.%m.%Y') if hasattr(end_date, 'strftime') else end_date}")
        
        # Метрики для выбранных параметров
        for i, metric in enumerate(selected_metrics, 2):
            if metric in df.columns:
                col_name = metric_labels.get(metric, metric)
                unit = metric_units.get(metric, "")
                avg_value = df[metric].mean()
                with metric_cols[i]:
                    st.metric(f"{col_name} (сред.)", f"{avg_value:.1f} {unit}".strip())
        
        st.markdown("---")
        
        # Вкладки
        tab1, tab2, tab3, tab4, tab5 = st.tabs([
            "📊 Графики", 
            "🗺️ География", 
            "📈 Сравнение", 
            "📋 Агрегированные данные", 
            "🗃️ Сырые данные"
        ])
        
        with tab1:
            st.header("Временные ряды")
            
            if selected_city != "All":
                city_df = df[df['city'] == selected_city]
                
                if not city_df.empty and selected_metrics:
                    # Создаем подграфики для выбранных метрик
                    fig = make_subplots(
                        rows=len(selected_metrics), 
                        cols=1, 
                        shared_xaxes=True,
                        vertical_spacing=0.05, 
                        subplot_titles=[
                            f"{metric_labels.get(m, m)}" + (f" ({metric_units.get(m)})" if metric_units.get(m) else "")
                            for m in selected_metrics
                        ]
                    )
                    
                    row = 1
                    colors = ['red', 'blue', 'green', 'purple', 'orange', 'brown']
                    
                    for i, metric in enumerate(selected_metrics):
                        if metric in city_df.columns:
                            fig.add_trace(
                                go.Scatter(
                                    x=city_df['time'],
                                    y=city_df[metric],
                                    name=metric_labels.get(metric, metric),
                                    line=dict(color=colors[i % len(colors)], width=2),
                                    mode='lines'
                                ),
                                row=row, col=1
                            )
                            
                            # Добавляем скользящее среднее для температуры
                            if metric == "temperature" and len(city_df) > 24:
                                city_df['temp_ma24h'] = city_df['temperature'].rolling(window=24, min_periods=1).mean()
                                fig.add_trace(
                                    go.Scatter(
                                        x=city_df['time'],
                                        y=city_df['temp_ma24h'],
                                        name="24ч скользящее среднее",
                                        line=dict(color='darkred', dash='dash', width=2),
                                        opacity=0.8
                                    ),
                                    row=row, col=1
                                )
                            
                            row += 1
                    
                    fig.update_layout(
                        height=250 * len(selected_metrics), 
                        title_text=f"Метеорологические данные: {selected_city}",
                        hovermode="x unified"
                    )
                    fig.update_xaxes(title_text="Время", row=len(selected_metrics), col=1)
                    
                    st.plotly_chart(fig, use_container_width=True)
            else:
                # Сравнение по всем городам для первой выбранной метрики
                if selected_metrics:
                    metric = selected_metrics[0]
                    if metric in df.columns:
                        # Берем последние 7 дней для наглядности
                        recent_df = df[df['time'] >= (datetime.now() - timedelta(days=7))]
                        
                        if not recent_df.empty:
                            fig = px.line(
                                recent_df,
                                x='time',
                                y=metric,
                                color='city',
                                title=f'Сравнение {metric} по городам (последние 7 дней)',
                                labels={
                                    'temperature': 'Температура (°C)',
                                    'humidity': 'Влажность (%)',
                                    'precipitation': 'Осадки (мм)',
                                    'wind_speed': 'Скорость ветра (м/с)',
                                    'pressure': 'Давление (гПа)',
                                    'cloud_cover': 'Облачность (%)'
                                }[metric],
                                line_group="city"  # Группируем линии по городам
                            )
                            fig.update_layout(height=600, hovermode="x unified")
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Добавляем статистику по городам
                            st.subheader("Статистика по городам за период")
                            city_stats = recent_df.groupby('city')[metric].agg(['mean', 'min', 'max', 'std']).reset_index()
                            city_stats.columns = ['Город', 'Среднее', 'Минимум', 'Максимум', 'Стандартное отклонение']
                            
                            # Форматируем числа
                            for col in ['Среднее', 'Минимум', 'Максимум', 'Стандартное отклонение']:
                                city_stats[col] = city_stats[col].apply(lambda x: f"{x:.2f}")
                            
                            st.dataframe(city_stats, use_container_width=True)
        
        with tab2:
            st.header("Географическое расположение городов")
            
            # Загружаем последние данные для всех городов
            agg_df = cached_get_aggregated_data()
            
            if not agg_df.empty:
                # Подготавливаем данные для карты
                map_data = []
                for city in CITY_COORDINATES.keys():
                    if city in agg_df['city'].values:
                        city_row = agg_df[agg_df['city'] == city].iloc[0]
                        map_data.append({
                            "city": city,
                            "lat": CITY_COORDINATES[city]["lat"],
                            "lon": CITY_COORDINATES[city]["lon"],
                            "temp_mean": city_row.get("temp_mean", 0),
                            "humidity_mean": city_row.get("humidity_mean", 0),
                            "precipitation_sum": city_row.get("precipitation_sum", 0),
                            "wind_speed_mean": city_row.get("wind_speed_mean", 0),
                            "last_updated": city_row.get("last_updated", "N/A")
                        })
                
                map_df = pd.DataFrame(map_data)
                
                if not map_df.empty:
                    # Выбор метрики для отображения на карте
                    map_metric = st.selectbox("Метрика для визуализации на карте", 
                                             ["temp_mean", "humidity_mean", "precipitation_sum", "wind_speed_mean"],
                                             format_func=lambda x: {
                                                 "temp_mean": "Средняя температура",
                                                 "humidity_mean": "Средняя влажность",
                                                 "precipitation_sum": "Сумма осадков",
                                                 "wind_speed_mean": "Средняя скорость ветра"
                                             }[x])
                    
                    # Определяем цветовую шкалу в зависимости от метрики
                    color_scale = "RdYlBu_r" if map_metric == "temp_mean" else "Blues"
                    
                    # Создаем карту
                    fig_map = px.scatter_mapbox(
                        map_df,
                        lat="lat",
                        lon="lon",
                        size=[10] * len(map_df),  # Размер точек
                        color=map_metric,
                        hover_name="city",
                        hover_data={
                            "lat": False,
                            "lon": False,
                            "temp_mean": ":.1f°C",
                            "humidity_mean": ":.1f%",
                            "precipitation_sum": ":.1f мм",
                            "wind_speed_mean": ":.1f м/с",
                            "last_updated": True
                        },
                        color_continuous_scale=color_scale,
                        zoom=1,
                        title=f"Глобальное распределение: {map_metric.replace('_', ' ').replace('mean', 'средняя').title()}"
                    )
                    
                    fig_map.update_layout(
                        mapbox_style="carto-positron", 
                        height=700,
                        margin={"r":0,"t":30,"l":0,"b":0}
                    )
                    
                    st.plotly_chart(fig_map, use_container_width=True)
                    
                    # Добавляем статистику по континентам
                    st.subheader("Средние показатели по континентам")
                    
                    # Группируем города по континентам
                    continent_mapping = {
                        # Европа
                        **{city: "Европа" for city in ["London", "Berlin", "Paris", "Madrid", "Moscow", "Rome", "Stockholm", "Athens", "Vienna", "Amsterdam", "Prague", "Warsaw", "Oslo", "Helsinki"]},
                        # Северная Америка
                        **{city: "Северная Америка" for city in ["New York", "Los Angeles", "Chicago", "Toronto", "Mexico City", "Vancouver", "Miami", "San Francisco", "Boston", "Denver"]},
                        # Азия и Тихий океан
                        **{city: "Азия и Океания" for city in ["Tokyo", "Singapore", "Mumbai", "Sydney", "Seoul", "Beijing", "Shanghai", "Bangkok", "Kuala Lumpur", "Auckland"]},
                        # Южная Америка и Африка
                        **{city: "Южная Америка и Африка" for city in ["Rio de Janeiro", "Sao Paulo", "Buenos Aires", "Lima", "Cairo", "Johannesburg", "Cape Town", "Nairobi", "Lagos"]}
                    }
                    
                    continent_data = []
                    for city in map_df['city']:
                        if city in continent_mapping:
                            continent_data.append({
                                "continent": continent_mapping[city],
                                "temp_mean": map_df[map_df['city'] == city]['temp_mean'].values[0],
                                "humidity_mean": map_df[map_df['city'] == city]['humidity_mean'].values[0],
                                "precipitation_sum": map_df[map_df['city'] == city]['precipitation_sum'].values[0],
                                "wind_speed_mean": map_df[map_df['city'] == city]['wind_speed_mean'].values[0]
                            })
                    
                    if continent_data:
                        continent_df = pd.DataFrame(continent_data)
                        continent_stats = continent_df.groupby('continent').mean().reset_index()
                        
                        fig_continent = px.bar(
                            continent_stats,
                            x='continent',
                            y='temp_mean',
                            color='temp_mean',
                            color_continuous_scale='RdYlBu_r',
                            title='Средняя температура по континентам',
                            labels={'continent': 'Континент', 'temp_mean': 'Средняя температура (°C)'}
                        )
                        fig_continent.update_layout(height=400)
                        st.plotly_chart(fig_continent, use_container_width=True)
                else:
                    st.info("Нет данных для отображения на карте. Запустите анализ для всех городов.")
            else:
                st.info("Нет агрегированных данных. Запустите анализ через `/etl/analyze`")
        
        with tab3:
            st.header("Подробное сравнение")
            
            agg_df = cached_get_aggregated_data()
            
            if not agg_df.empty:
                # Разделение на метрики
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Выбор метрики для сравнения")
                    comparison_metric = st.selectbox(
                        "Основная метрика", 
                        ["temp_mean", "humidity_mean", "precipitation_sum", "wind_speed_mean", "pressure_mean", "cloud_cover_mean"],
                        format_func=lambda x: {
                            "temp_mean": "🌡️ Средняя температура (°C)",
                            "humidity_mean": "💧 Средняя влажность (%)",
                            "precipitation_sum": "🌧️ Сумма осадков (мм)",
                            "wind_speed_mean": "💨 Средняя скорость ветра (м/с)",
                            "pressure_mean": "📉 Среднее давление (гПа)",
                            "cloud_cover_mean": "☁️ Средняя облачность (%)"
                        }[x]
                    )
                
                with col2:
                    st.subheader("Дополнительная метрика")
                    secondary_metric = st.selectbox(
                        "Вторая метрика для сравнения", 
                        ["temp_mean", "humidity_mean", "precipitation_sum", "wind_speed_mean", "pressure_mean", "cloud_cover_mean"],
                        index=1,
                        format_func=lambda x: {
                            "temp_mean": "🌡️ Средняя температура (°C)",
                            "humidity_mean": "💧 Средняя влажность (%)",
                            "precipitation_sum": "🌧️ Сумма осадков (мм)",
                            "wind_speed_mean": "💨 Средняя скорость ветра (м/с)",
                            "pressure_mean": "📉 Среднее давление (гПа)",
                            "cloud_cover_mean": "☁️ Средняя облачность (%)"
                        }[x]
                    )
                
                # Сортировка данных
                agg_df_sorted = agg_df.sort_values(by=comparison_metric, ascending=(comparison_metric != "temp_mean"))
                
                # Создаем график сравнения
                fig_comparison = px.bar(
                    agg_df_sorted,
                    x='city',
                    y=comparison_metric,
                    color=comparison_metric,
                    color_continuous_scale='RdYlBu_r' if comparison_metric == "temp_mean" else 'Blues',
                    title=f'Сравнение: {comparison_metric.replace("_", " ").title()}',
                    labels={
                        'city': 'Город',
                        comparison_metric: comparison_metric.replace("_", " ").replace("mean", "средняя").replace("sum", "сумма").title()
                    },
                    text=comparison_metric
                )
                
                fig_comparison.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                fig_comparison.update_layout(height=500, margin=dict(t=50, b=100))
                fig_comparison.update_xaxes(tickangle=45)
                
                st.plotly_chart(fig_comparison, use_container_width=True)
                
                # Scatter plot для сравнения двух метрик
                st.subheader(f"Корреляция: {comparison_metric} vs {secondary_metric}")
                
                fig_scatter = px.scatter(
                    agg_df,
                    x=comparison_metric,
                    y=secondary_metric,
                    color="temp_mean" if comparison_metric != "temp_mean" and secondary_metric != "temp_mean" else None,
                    size="precipitation_sum" if "precipitation_sum" in agg_df.columns else None,
                    hover_name="city",
                    title=f"Сравнение: {comparison_metric.replace('_', ' ')} vs {secondary_metric.replace('_', ' ')}",
                    color_continuous_scale='RdYlBu_r',
                    labels={
                        comparison_metric: comparison_metric.replace("_", " ").title(),
                        secondary_metric: secondary_metric.replace("_", " ").title()
                    }
                )
                
                fig_scatter.update_layout(height=500)
                st.plotly_chart(fig_scatter, use_container_width=True)
                
                # Корреляционная матрица
                st.subheader("Корреляционная матрица метеорологических метрик")
                
                # Выбираем числовые колонки для корреляции
                corr_columns = ['temp_mean', 'humidity_mean', 'precipitation_sum', 'wind_speed_mean', 'pressure_mean', 'cloud_cover_mean']
                corr_columns = [col for col in corr_columns if col in agg_df.columns]
                
                if len(corr_columns) > 1:
                    corr_df = agg_df[corr_columns].copy()
                    corr_matrix = corr_df.corr()
                    
                    fig_corr = px.imshow(
                        corr_matrix,
                        text_auto=True,
                        aspect='auto',
                        color_continuous_scale='RdBu_r',
                        title='Корреляция между метеорологическими метриками'
                    )
                    
                    st.plotly_chart(fig_corr, use_container_width=True)
                else:
                    st.info("Недостаточно метрик для построения корреляционной матрицы")
            else:
                st.warning("Нет агрегированных данных для сравнения")
        
        with tab4:
            st.header("Агрегированная статистика")
            
            agg_df = cached_get_aggregated_data()
            
            if not agg_df.empty:
                # Фильтры для таблицы
                col1, col2, col3 = st.columns(3)
                with col1:
                    sort_by_agg = st.selectbox("Сортировать по", agg_df.columns.tolist(), index=0)
                with col2:
                    sort_order_agg = st.selectbox("Порядок", ['По убыванию', 'По возрастанию'])
                with col3:
                    num_cities = st.slider("Количество городов для отображения", 5, min(50, len(agg_df)), 20)
                
                ascending_agg = sort_order_agg == 'По возрастанию'
                agg_df_sorted = agg_df.sort_values(by=sort_by_agg, ascending=ascending_agg)
                
                # Отображаем топ-N городов
                st.subheader(f"Топ-{num_cities} городов")
                st.dataframe(
                    agg_df_sorted.head(num_cities),
                    use_container_width=True,
                    height=400
                )
                
                # Экспорт данных
                csv_agg = agg_df_sorted.to_csv(index=False)
                st.download_button(
                    label="📥 Скачать агрегированные данные CSV",
                    data=csv_agg,
                    file_name="weather_aggregated_data.csv",
                    mime="text/csv"
                )
                
                # Статистика по всем данным
                st.subheader("Общая статистика по всем городам")
                stats_cols = st.columns(3)
                
                for i, metric in enumerate(['temp_mean', 'humidity_mean', 'precipitation_sum']):
                    if metric in agg_df.columns:
                        with stats_cols[i]:
                            metric_name = {
                                "temp_mean": "Температура",
                                "humidity_mean": "Влажность",
                                "precipitation_sum": "Осадки"
                            }[metric]
                            
                            unit = {
                                "temp_mean": "°C",
                                "humidity_mean": "%",
                                "precipitation_sum": "мм"
                            }[metric]
                            
                            st.metric(
                                f"📊 Средняя {metric_name}",
                                f"{agg_df[metric].mean():.2f} {unit}"
                            )
                            st.metric(
                                f"🔥 Максимум {metric_name}",
                                f"{agg_df[metric].max():.2f} {unit}"
                            )
                            st.metric(
                                f"❄️ Минимум {metric_name}",
                                f"{agg_df[metric].min():.2f} {unit}"
                            )
            else:
                st.warning("Нет агрегированных данных. Запустите анализ через `/etl/analyze`")
        
        with tab5:
            st.header("Сырые данные")
            
            # Фильтры для таблицы
            col1, col2, col3 = st.columns(3)
            with col1:
                sort_by = st.selectbox("Сортировка", ['time', 'city', 'temperature', 'humidity', 'precipitation', 'wind_speed', 'pressure', 'cloud_cover'], index=0)
            with col2:
                sort_order = st.selectbox("Порядок сортировки", ['По убыванию', 'По возрастанию'])
            with col3:
                page_size = st.selectbox("Записей на странице", [50, 100, 200, 500, 1000], index=1)
            
            ascending = sort_order == 'По возрастанию'
            df_sorted = df.sort_values(by=sort_by, ascending=ascending)
            
            # Пагинация
            total_pages = max(1, len(df_sorted) // page_size + 1)
            page_num = st.number_input("Страница", min_value=1, max_value=total_pages, value=1)
            start_idx = (page_num - 1) * page_size
            end_idx = min(start_idx + page_size, len(df_sorted))
            
            st.markdown(f"Показано записей: {start_idx + 1} - {end_idx} из {len(df_sorted)}")
            
            # Показываем только основные колонки для наглядности
            display_columns = ['time', 'city', 'temperature', 'humidity']
            for metric in ['precipitation', 'wind_speed', 'pressure', 'cloud_cover']:
                if metric in df.columns:
                    display_columns.append(metric)
            
            st.dataframe(
                df_sorted[display_columns].iloc[start_idx:end_idx],
                use_container_width=True,
                height=450
            )
            
            # Скачать данные
            csv = df_sorted.to_csv(index=False)
            st.download_button(
                label="📥 Скачать CSV",
                data=csv,
                file_name="weather_data.csv",
                mime="text/csv"
            )
            
            # Статистика по данным
            st.subheader("Статистика по данным")
            stats_cols = st.columns(3)
            
            with stats_cols[0]:
                st.metric("🏙️ Уникальных городов", df['city'].nunique())
            
            with stats_cols[1]:
                date_range = f"{df['time'].min().strftime('%d.%m.%Y')} - {df['time'].max().strftime('%d.%m.%Y')}"
                st.metric("📅 Диапазон дат", date_range)
            
            with stats_cols[2]:
                st.metric("🧠 Объем данных", f"{len(df)} записей")

except Exception as e:
    st.error(f"Ошибка при загрузке данных: {str(e)}")
    st.exception(e)
    st.info("Убедитесь, что PostgreSQL запущен и данные загружены")

# Футер
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    <p>World Weather Analytics Dashboard | Powered by Streamlit, Dask, PostgreSQL, Prefect</p>
    <p>Данные предоставлены Open-Meteo API | Обновление каждые 5 минут</p>
</div>
""", unsafe_allow_html=True)

# Автообновление каждые 5 минут
st_autorefresh = st.empty()
st_autorefresh.markdown(f"Последнее обновление: {datetime.now().strftime('%H:%M:%S')}")
