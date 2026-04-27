import streamlit as st
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# Chargement des variables d'environnement
load_dotenv()

# Connexion à la base de données
DATABASE_URL = os.getenv("DATABASE_URL")

@st.cache_data(ttl=60)
def get_data(query):
    try:
        conn = psycopg2.connect(DATABASE_URL)
        df = pd.read_sql(query, conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données : {e}")
        return pd.DataFrame()

# Configuration de la page
st.set_page_config(page_title="Dashboard d'Analyse de Marché", layout="wide")

# CSS personnalisé pour l'espacement et les couleurs
st.markdown("""
    <style>
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .stMetric {
        background-color: #17212b;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #000000;
    }
    </style>
    """, unsafe_allow_html=True)

# En-tête
st.title("Dashboard d'Analyse de Marché")
st.markdown("Visualisation détaillée de l'impact des actualités sur les marchés de la Crypto, du Pétrole et de l'Or.")

# Onglets
tab_market, tab_system = st.tabs(["Analyse de Marché", "Analyse du Système"])

with tab_market:
    # Récupération des données de marché
    query = """
    SELECT 
        f.event_time,
        a.symbol,
        n.title as headline,
        n.news_id,
        f.price_at_event,
        f.price_24h,
        f.return_1h,
        f.return_4h,
        f.return_24h,
        f.spike_flag
    FROM fact_news_market f
    JOIN dim_asset a ON f.asset_id = a.asset_id
    JOIN dim_news n ON f.news_id = n.news_id
    ORDER BY f.event_time DESC
    """
    df = get_data(query)

    if df.empty:
        st.warning("Aucune donnée de marché disponible pour le moment.")
    else:
        # 1. Résumé des indicateurs
        st.markdown("### Résumé du Marché")
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Événements Totaux", len(df))
        m2.metric("Pics de Volatilité", int(df['spike_flag'].sum()))
        m3.metric("Variation Moyenne (1h)", f"{df['return_1h'].mean():.2%}")
        m4.metric("Variation Moyenne (4h)", f"{df['return_4h'].mean():.2%}")
        m5.metric("Variation Moyenne (24h)", f"{df['return_24h'].mean():.2%}")

        st.markdown("---")

        # Ligne 1 : Intensité de réaction et Chronologie
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### Réaction Moyenne par Actif")
            df['abs_return_1h'] = df['return_1h'].abs()
            avg_by_asset = df.groupby('symbol')['abs_return_1h'].mean().reset_index()
            fig_bar = px.bar(
                avg_by_asset, x='symbol', y='abs_return_1h',
                title="Intensité de la réaction (Moyenne absolue)",
                labels={'abs_return_1h': 'Variation Moyenne', 'symbol': 'Actif'},
                template="plotly_dark",
                color='symbol'
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        with col2:
            st.markdown("### Impact sur les Prix dans le Temps")
            fig_scatter = px.scatter(
                df, x='event_time', y='return_1h', color='symbol',
                title="Variation des prix (%) par événement",
                labels={'return_1h': 'Variation (%)', 'event_time': 'Temps'},
                template="plotly_dark",
                hover_data=['headline']
            )
            st.plotly_chart(fig_scatter, use_container_width=True)

        st.markdown("---")

        # Ligne 2 : Analyse des seuils et Répartition
        col3, col4 = st.columns(2)

        with col3:
            st.markdown("### Analyse des Pics de Volatilité")
            fig_spike = go.Figure()
            fig_spike.add_trace(go.Scatter(x=df['event_time'], y=df['return_1h'].abs(), mode='markers', name='Magnitude'))
            fig_spike.add_hline(y=0.005, line_dash="dash", line_color="red", annotation_text="Seuil 0.005")
            fig_spike.update_layout(title="Réactions vs Seuil de sensibilité", template="plotly_dark", xaxis_title="Temps", yaxis_title="Variation Absolue")
            st.plotly_chart(fig_spike, use_container_width=True)

        with col4:
            st.markdown("### Répartition des Pics par Actif")
            all_symbols = pd.DataFrame({'symbol': df['symbol'].unique()})
            spike_counts = df[df['spike_flag'] == True].groupby('symbol').size().reset_index(name='spike_count')
            spike_data = all_symbols.merge(spike_counts, on='symbol', how='left').fillna(0)
            
            fig_spikes = px.bar(
                spike_data, x='symbol', y='spike_count',
                title="Nombre total de pics détectés",
                labels={'spike_count': 'Nombre de pics', 'symbol': 'Actif'},
                template="plotly_dark",
                color='symbol'
            )
            st.plotly_chart(fig_spikes, use_container_width=True)

        st.markdown("---")

        # Ligne 3 : Analyse des actualités marquantes
        st.markdown("### Analyse des Actualités Les plus intéressantes")
        notable_ids = [
            'ca4605fc-a00f-4e70-a402-66338755d286',
            '4512477b-8727-4c57-94de-0ba48c747d52',
            '65e919a4-2116-4084-9e63-7bd878967428',
            '4ced66a3-8c78-4bee-b545-b32c2e05b341',
            'b560c031-313d-461f-bfb1-aa56c184b27f'
        ]
        
        notable_data = df[df['news_id'].astype(str).isin(notable_ids)]
        
        if not notable_data.empty:
            for nid in notable_ids:
                news_group = notable_data[notable_data['news_id'].astype(str) == nid]
                if not news_group.empty:
                    headline = news_group['headline'].iloc[0]
                    st.markdown(f"#### {headline}")
                    st.table(news_group[['symbol', 'price_at_event', 'price_24h', 'return_24h']].rename(columns={
                        'symbol': 'Actif',
                        'price_at_event': 'Prix à l\'événement',
                        'price_24h': 'Prix après 24h',
                        'return_24h': 'Variation 24h'
                    }))
        else:
            st.info("Aucune actualité marquante trouvée avec les IDs spécifiés.")

with tab_system:
    st.markdown("### Surveillance des Performances du Système")
    
    # 1. Section des exécutions du pipeline
    st.header("Cycles d'Exécution du Pipeline")
    runs_query = "SELECT source_name, status, start_time, end_time FROM pipeline_runs ORDER BY start_time DESC"
    runs_df = get_data(runs_query)
    
    if not runs_df.empty:
        runs_df['start_time'] = pd.to_datetime(runs_df['start_time'])
        runs_df['end_time'] = pd.to_datetime(runs_df['end_time'])
        runs_df['duration'] = (runs_df['end_time'] - runs_df['start_time']).dt.total_seconds().apply(
            lambda x: f"{int(x // 60)}m {int(x % 60)}s" if pd.notnull(x) else "Actif"
        )
        st.dataframe(runs_df[['source_name', 'status', 'start_time', 'end_time', 'duration']].rename(columns={
            'source_name': 'Source',
            'status': 'Statut',
            'start_time': 'Début',
            'end_time': 'Fin',
            'duration': 'Durée'
        }), use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # 2. Section des tâches
    st.header("Analyse des Tâches du Système")
    
    # Requêtes SQL directes pour une fiabilité maximale
    ingestion_query = """
    SELECT task_name, records_count, start_time 
    FROM tasks 
    WHERE task_name ILIKE '%ingestion%' 
    AND status ILIKE 'SUCCESS'
    ORDER BY start_time DESC
    """
    ingestion_df = get_data(ingestion_query)

    errors_query = "SELECT task_name, start_time, error_message FROM tasks WHERE status ILIKE 'FAILED' ORDER BY start_time DESC"
    failures_df = get_data(errors_query)

    all_tasks_query = "SELECT task_name, status, records_count, start_time, error_message FROM tasks ORDER BY start_time DESC"
    tasks_df = get_data(all_tasks_query)

    if not tasks_df.empty:
        col_s1, col_s2 = st.columns(2)
        
        with col_s1:
            st.markdown("#### Volumes des records ajoutés à chaque ingestion")
            if not ingestion_df.empty:
                ingestion_df['records_count'] = pd.to_numeric(ingestion_df['records_count'], errors='coerce').fillna(0)
                ingestion_df['time_str'] = pd.to_datetime(ingestion_df['start_time']).dt.strftime('%Y-%m-%d %H:%M')
                
                fig_vol = px.area(
                    ingestion_df, x='time_str', y='records_count', 
                    title="Volume des records par ingestion", 
                    template="plotly_dark",
                    labels={'records_count': 'Lignes ajoutées', 'time_str': 'Heure d\'exécution'},
                    markers=True,
                    color_discrete_sequence=['#00d4ff']
                )
                st.plotly_chart(fig_vol, use_container_width=True)
            else:
                st.info("Aucune donnée d'ingestion réussie détectée.")
            
        with col_s2:
            st.markdown("#### Alertes d'Erreurs Système")
            if not failures_df.empty:
                st.dataframe(failures_df.rename(columns={
                    'task_name': 'Tâche',
                    'start_time': 'Heure',
                    'error_message': 'Message d\'erreur'
                }), use_container_width=True, hide_index=True)
            else:
                st.success("Le système est sain. Aucune erreur détectée.")

        st.markdown("---")
        st.markdown("#### Historique complet des tâches")
        st.dataframe(tasks_df[['task_name', 'status', 'records_count', 'start_time', 'error_message']], use_container_width=True, hide_index=True)
    else:
        st.info("Aucun journal de tâches disponible.")
