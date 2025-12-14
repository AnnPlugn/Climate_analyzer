"""
Streamlit приложение для визуализации климатических данных
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import os
from app.database import init_db, load_data_from_db, get_aggregated_data

# Настройка страницы
st.set_page_config(
    page_title="Weather Data Analytics",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Инициализация БД с обработкой ошибок
try:
    init_db()
except Exception as e:
    st.error(f"Ошибка инициализации БД: {str(e)}")

# Кэширование функции получения агрегированных данных
@st.cache_data(ttl=60)  # Кэш на 60 секунд
def cached_get_aggregated_data():
    """Кэшированная функция для получения агрегированных данных"""
    try:
        return get_aggregated_data()
    except Exception as e:
        st.error(f"Ошибка загрузки агрегированных данных: {str(e)}")
        return pd.DataFrame()

# Заголовок
st.title("🌡️ Weather Data Analytics Dashboard")
st.markdown("---")

# Боковая панель с фильтрами
st.sidebar.header("📊 Фильтры и настройки")

# Выбор города
cities = ["All"] + ["London", "Berlin", "Madrid", "Moscow", "Paris"]
selected_city = st.sidebar.selectbox("Выберите город", cities)

# Количество записей для отображения
limit = st.sidebar.slider("Количество записей", 100, 10000, 1000, 100)

# Основной контент
try:
    # Загрузка данных
    if selected_city == "All":
        df = load_data_from_db(limit=limit)
    else:
        df = load_data_from_db(city=selected_city, limit=limit)
    
    if df.empty:
        st.warning("⚠️ Нет данных в базе. Запустите ETL процесс через API или Prefect.")
        st.info("💡 Используйте `/etl/ingest` endpoint для загрузки данных")
    else:
        # Преобразование времени
        df['time'] = pd.to_datetime(df['time'])
        
        # Метрики
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📈 Всего записей", len(df))
        
        with col2:
            avg_temp = df['temperature'].mean()
            st.metric("🌡️ Средняя температура", f"{avg_temp:.1f}°C")
        
        with col3:
            max_temp = df['temperature'].max()
            st.metric("🔥 Макс. температура", f"{max_temp:.1f}°C")
        
        with col4:
            avg_humidity = df['humidity'].mean()
            st.metric("💧 Средняя влажность", f"{avg_humidity:.1f}%")
        
        st.markdown("---")
        
        # Вкладки
        tab1, tab2, tab3, tab4 = st.tabs(["📊 Графики", "🌍 По городам", "📈 Агрегированные данные", "📋 Сырые данные"])
        
        with tab1:
            st.header("Временные ряды")
            
            # График температуры
            fig_temp = px.line(
                df,
                x='time',
                y='temperature',
                color='city',
                title='Температура по времени',
                labels={'temperature': 'Температура (°C)', 'time': 'Время'}
            )
            fig_temp.update_layout(height=400)
            st.plotly_chart(fig_temp, use_container_width=True)
            
            # График влажности
            fig_hum = px.line(
                df,
                x='time',
                y='humidity',
                color='city',
                title='Влажность по времени',
                labels={'humidity': 'Влажность (%)', 'time': 'Время'}
            )
            fig_hum.update_layout(height=400)
            st.plotly_chart(fig_hum, use_container_width=True)
            
            # Комбинированный график
            if selected_city != "All":
                fig_combined = make_subplots(
                    rows=2, cols=1,
                    subplot_titles=('Температура', 'Влажность'),
                    vertical_spacing=0.1
                )
                
                city_df = df[df['city'] == selected_city]
                
                fig_combined.add_trace(
                    go.Scatter(
                        x=city_df['time'],
                        y=city_df['temperature'],
                        name='Температура',
                        line=dict(color='red')
                    ),
                    row=1, col=1
                )
                
                fig_combined.add_trace(
                    go.Scatter(
                        x=city_df['time'],
                        y=city_df['humidity'],
                        name='Влажность',
                        line=dict(color='blue')
                    ),
                    row=2, col=1
                )
                
                fig_combined.update_xaxes(title_text="Время", row=2, col=1)
                fig_combined.update_yaxes(title_text="Температура (°C)", row=1, col=1)
                fig_combined.update_yaxes(title_text="Влажность (%)", row=2, col=1)
                fig_combined.update_layout(height=600, title_text=f"Детальный анализ: {selected_city}")
                
                st.plotly_chart(fig_combined, use_container_width=True)
        
        with tab2:
            st.header("Сравнение городов")
            
            # Загрузка агрегированных данных
            agg_df = cached_get_aggregated_data()
            
            # Отладочная информация
            with st.expander("🔍 Отладочная информация", expanded=False):
                st.write(f"Количество записей: {len(agg_df)}")
                st.write(f"Пустой DataFrame: {agg_df.empty}")
                if not agg_df.empty:
                    st.write("Первые строки:", agg_df.head())
            
            if not agg_df.empty and len(agg_df) > 0:
                # График средних температур
                fig_bar = px.bar(
                    agg_df,
                    x='city',
                    y='temp_mean',
                    title='Средняя температура по городам',
                    labels={'temp_mean': 'Температура (°C)', 'city': 'Город'},
                    color='temp_mean',
                    color_continuous_scale='RdYlBu_r'
                )
                fig_bar.update_layout(height=400)
                st.plotly_chart(fig_bar, use_container_width=True)
                
                # Box plot по городам
                fig_box = px.box(
                    df,
                    x='city',
                    y='temperature',
                    title='Распределение температур по городам',
                    labels={'temperature': 'Температура (°C)', 'city': 'Город'}
                )
                fig_box.update_layout(height=400)
                st.plotly_chart(fig_box, use_container_width=True)
                
                # Тепловая карта метрик
                metrics_df = agg_df.set_index('city')[['temp_mean', 'temp_max', 'temp_min', 'humidity_mean']]
                fig_heatmap = px.imshow(
                    metrics_df.T,
                    labels=dict(x="Город", y="Метрика", color="Значение"),
                    title="Тепловая карта метрик по городам",
                    aspect="auto",
                    color_continuous_scale='Viridis'
                )
                st.plotly_chart(fig_heatmap, use_container_width=True)
            else:
                st.info("Нет агрегированных данных. Запустите анализ через `/etl/analyze`")
        
        with tab3:
            st.header("Агрегированная статистика")
            
            # Загрузка агрегированных данных
            agg_df = cached_get_aggregated_data()
            
            # Отладочная информация
            with st.expander("🔍 Отладочная информация", expanded=False):
                st.write(f"Количество записей: {len(agg_df)}")
                st.write(f"Пустой DataFrame: {agg_df.empty}")
                if not agg_df.empty:
                    st.write("Данные:", agg_df)
            
            if not agg_df.empty and len(agg_df) > 0:
                # Таблица с агрегированными данными
                st.dataframe(
                    agg_df[['city', 'temp_mean', 'temp_max', 'temp_min', 'humidity_mean', 'last_updated']],
                    use_container_width=True
                )
                
                # Статистика по городам
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("Температура")
                    st.dataframe(
                        agg_df[['city', 'temp_mean', 'temp_max', 'temp_min']].round(2),
                        use_container_width=True
                    )
                
                with col2:
                    st.subheader("Влажность")
                    st.dataframe(
                        agg_df[['city', 'humidity_mean']].round(2),
                        use_container_width=True
                    )
            else:
                st.warning("Нет агрегированных данных")
        
        with tab4:
            st.header("Сырые данные")
            
            # Фильтры для таблицы
            col1, col2 = st.columns(2)
            with col1:
                sort_by = st.selectbox("Сортировка", ['time', 'temperature', 'humidity', 'city'])
            with col2:
                sort_order = st.selectbox("Порядок", ['По убыванию', 'По возрастанию'])
            
            ascending = sort_order == 'По возрастанию'
            df_sorted = df.sort_values(by=sort_by, ascending=ascending)
            
            st.dataframe(
                df_sorted[['time', 'city', 'temperature', 'humidity']],
                use_container_width=True,
                height=400
            )
            
            # Скачать данные
            csv = df_sorted.to_csv(index=False)
            st.download_button(
                label="📥 Скачать CSV",
                data=csv,
                file_name="weather_data.csv",
                mime="text/csv"
            )

except Exception as e:
    st.error(f"Ошибка при загрузке данных: {str(e)}")
    st.info("Убедитесь, что PostgreSQL запущен и данные загружены")

# Футер
st.markdown("---")
st.markdown("""
<div style='text-align: center; color: gray;'>
    <p>Weather Data Analytics Dashboard | Powered by Streamlit, Dask, PostgreSQL, Prefect</p>
</div>
""", unsafe_allow_html=True)

