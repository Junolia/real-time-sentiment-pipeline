import streamlit as st
import pandas as pd
import plotly.express as px
import json
from collections import Counter
from kafka import KafkaConsumer

# 1. Page Config
st.set_page_config(page_title="Platform Macro Sentiment", layout="wide")

# Custom CSS for clean UI
st.markdown("""
    <style>
    .stApp { background-color: #FDFDFD; }
    h1, h2, h3, p, span { color: #2C3E50 !important; font-family: 'Inter', sans-serif; }
    .metric-card {
        background-color: #FDFBF7; padding: 20px; border-radius: 8px; 
        border: 1px solid #EAEAEA; text-align: center;
    }
    .metric-value { font-size: 38px; font-weight: bold; color: #34495E; }
    .metric-label { font-size: 14px; color: #7F8C8D; text-transform: uppercase; }
    </style>
""", unsafe_allow_html=True)

# English Stop Words for cleaning mentioned words
STOP_WORDS = {
    "the", "and", "to", "of", "a", "in", "is", "that", "it", "for", "you", "i", "on", 
    "this", "but", "with", "as", "have", "not", "what", "just", "if", "we", "all", "my", "they"
    , "your", "about", "it's", "would", "will", "from", "because", "people", "please", "their", "only", 
    "been", "when", "more", "make", "don't", "are", "even", "were", "some", "there", "think", "than", "know",
    "you're", "then", "other", "time", "really", "like", "also", "wants", "could", "being", "still", "need",
    "also", "much", "need", "should", "after", "someone", "dont", "should", "them", "does", "going",
    "very", "that's", "into"
}

# 2. Kafka Connection
@st.cache_resource
def get_consumer():
    return KafkaConsumer(
        'dashboard_feed',
        bootstrap_servers=['127.0.0.1:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        consumer_timeout_ms=50
    )

st.title("Reddit Platform Sentiment Analysis")
st.caption("Macro Analysis across 40 Subreddits via Spark Streaming")

# 3. Visualization Fragment
@st.fragment(run_every=2)
def render_dashboard():
    consumer = get_consumer()
    if 'buffer' not in st.session_state:
        st.session_state.buffer = []

    # Get latest batch from Kafka
    msg_pack = consumer.poll(timeout_ms=50, max_records=200)
    for tp, messages in msg_pack.items():
        for msg in messages:
            st.session_state.buffer.append(msg.value)
    
    # Maintain a sliding window of 3000 comments for analysis
    if len(st.session_state.buffer) > 3000:
        st.session_state.buffer = st.session_state.buffer[-3000:]

    if len(st.session_state.buffer) < 10:
        st.info("Waiting for data from Spark...")
        return

    df = pd.DataFrame(st.session_state.buffer)

    # Metrics calculation
    overall_avg = df['sentiment_score'].mean()
    
    # Grouping by Subreddit
    sub_stats = df.groupby('subreddit')['sentiment_score'].mean().reset_index()
    top_pos = sub_stats.sort_values('sentiment_score', ascending=False).head(5)
    top_neg = sub_stats.sort_values('sentiment_score', ascending=True).head(5)

    # Word Frequency calculation from cleaned content
    if 'content_clean' in df.columns:
        words = " ".join(df['content_clean'].dropna().astype(str)).split()
        filtered = [w for w in words if w not in STOP_WORDS and len(w) > 3]
        word_data = pd.DataFrame(Counter(filtered).most_common(10), columns=['Word', 'Count'])
    else:
        word_data = pd.DataFrame()

    # --- UI Layout ---
    
    # Top Row: Global Sentiment
    st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Global Platform Sentiment Average</div>
            <div class="metric-value">{overall_avg:.4f}</div>
        </div>
    """, unsafe_allow_html=True)
    st.write("")

    # Middle Row: Sentiment Rankings
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Top 5 Positive Subreddits")
        fig_pos = px.bar(top_pos, x='sentiment_score', y='subreddit', orientation='h', color_discrete_sequence=['#85C1E9'])
        st.plotly_chart(fig_pos, use_container_width=True)
        
    with col2:
        st.subheader("Top 5 Negative Subreddits")
        fig_neg = px.bar(top_neg, x='sentiment_score', y='subreddit', orientation='h', color_discrete_sequence=['#E74C3C'])
        st.plotly_chart(fig_neg, use_container_width=True)

    # Bottom Row: NLP Insights
    st.subheader("Most Mentioned Words")
    if not word_data.empty:
        fig_word = px.bar(word_data, x='Count', y='Word', orientation='h', color='Count', color_continuous_scale='Blues')
        fig_word.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig_word, use_container_width=True)

render_dashboard()