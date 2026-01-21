import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import requests
import json
import numpy as np
from datetime import datetime, timedelta
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

def get_naver_api_credentials():
    if "naver_api" in st.secrets:
        return st.secrets["naver_api"]["client_id"], st.secrets["naver_api"]["client_secret"]
    client_id = os.getenv("NAVER_CLIENT_ID")
    client_secret = os.getenv("NAVER_CLIENT_SECRET")
    return client_id, client_secret

@st.cache_data
def fetch_naver_trend(keywords, start_date, end_date):
    client_id, client_secret = get_naver_api_credentials()
    if not client_id or not client_secret or "your_client_id" in client_id:
        return None
    url = "https://openapi.naver.com/v1/datalab/search"
    headers = {
        "X-Naver-Client-Id": client_id,
        "X-Naver-Client-Secret": client_secret,
        "Content-Type": "application/json"
    }
    keyword_groups = [{"groupName": kw, "keywords": [kw]} for kw in keywords if kw.strip()]
    if not keyword_groups: return None
    body = {"startDate": start_date, "endDate": end_date, "timeUnit": "month", "keywordGroups": keyword_groups}
    try:
        response = requests.post(url, headers=headers, data=json.dumps(body))
        if response.status_code == 200: return response.json()
        return None
    except: return None

# 페이지 설정
st.set_page_config(page_title="Olist 데이터 분석 대시보드", layout="wide")

# 데이터 경로 설정
base_path = os.path.dirname(__file__)
DATA_PATH = os.path.join(base_path, 'DATA_1')
if not os.path.exists(DATA_PATH):
    DATA_PATH = base_path

@st.cache_data
def load_data():
    file_bases = {
        'orders': 'olist_orders_dataset',
        'order_items': 'olist_order_items_dataset',
        'order_reviews': 'olist_order_reviews_dataset',
        'products': 'olist_products_dataset',
        'payments': 'olist_order_payments_dataset',
        'customers': 'olist_customers_dataset',
        'sellers': 'olist_sellers_dataset',
        'translation': 'product_category_name_translation'
    }
    loaded_data = {}
    for key, base_name in file_bases.items():
        found = False
        for suffix in ['_cleaned', '']:
            for ext in ['.parquet', '.csv']:
                path = os.path.join(DATA_PATH, base_name + suffix + ext)
                if os.path.exists(path):
                    if ext == '.parquet':
                        loaded_data[key] = pd.read_parquet(path)
                    else:
                        loaded_data[key] = pd.read_csv(path)
                    found = True
                    break
            if found: break
        if not found:
            st.error(f"❌ '{base_name}' 파일을 찾을 수 없습니다.")
            st.stop()
    
    # 날짜 형식 변환
    orders = loaded_data['orders']
    date_cols = ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date']
    for col in date_cols:
        if col in orders.columns:
            orders[col] = pd.to_datetime(orders[col])
    
    return (loaded_data['orders'], loaded_data['order_items'], loaded_data['order_reviews'], 
            loaded_data['products'], loaded_data['payments'], loaded_data['customers'], 
            loaded_data['sellers'], loaded_data['translation'])

@st.cache_data
def get_korea_mock_data():
    kr_delivery = pd.DataFrame({
        '시도': ['서울', '경기', '인천', '부산', '대구', '대전', '광주', '강원', '제주'],
        '물동량': [1200, 1500, 800, 600, 400, 350, 300, 200, 150],
        '평균배송시간': [1.2, 1.5, 1.4, 1.8, 1.9, 1.7, 2.0, 2.5, 3.2]
    })
    kr_economy = pd.DataFrame({
        'month': pd.date_range(start='2017-01-01', periods=24, freq='MS').astype(str),
        'cpi': [100 + i*0.2 + np.random.normal(0, 0.1) for i in range(24)],
        'online_sales': [500 + i*10 + np.random.normal(0, 20) for i in range(24)]
    })
    kr_complaints = pd.DataFrame({
        'type': ['배송지연', '제품파손', '오배송', '환불거절', '기타'],
        'count': [45, 20, 15, 12, 8]
    })
    return kr_delivery, kr_economy, kr_complaints

# 데이터 로딩
with st.spinner('데이터를 불러오는 중...'):
    orders, order_items, order_reviews, products, payments, customers, sellers, translation = load_data()
    kr_delivery, kr_economy, kr_complaints = get_korea_mock_data()

# --- 사이드바 ---
st.sidebar.title("🔍 분석 옵션")
tab_selection = st.sidebar.radio("모드 선택", ["대시보드 메인", "OLIST-한국 비교"])

if tab_selection == "대시보드 메인":
    st.sidebar.header("🔍 전역 필터")
    search_query = st.sidebar.text_input("카테고리 키워드 검색", "")
    price_range = st.sidebar.slider("상품 가격 범위 필터 (BRL)", 0.0, 1000.0, (0.0, 500.0))
    
    # 데이터 필터링 (메인용)
    filtered_items = order_items[(order_items['price'] >= price_range[0]) & (order_items['price'] <= price_range[1])]
    if search_query:
        matching_cats = translation[translation['product_category_name_english'].str.contains(search_query, case=False, na=False)]['product_category_name'].tolist()
        filtered_products = products[products['product_category_name'].isin(matching_cats)]
    else:
        filtered_products = products

    st.title("📊 Olist 브라질 이커머스 인사이트 대시보드")
    m_tab1, m_tab2, m_tab3, m_tab4, m_tab5, m_tab6 = st.tabs([
        "🚚 배송 및 리뷰 분석", "📦 카테고리 및 취소율", "💳 결제 및 할부 분석", "🌎 지역별 매출 분석", "💡 심층 인사이트", "📈 네이버 트렌드 비교"
    ])

    with m_tab1:
        st.subheader("배송 소요일 구간별 평균 리뷰 점수")
        df_delivery = orders.dropna(subset=['order_delivered_customer_date']).copy()
        df_delivery['delivery_days'] = (df_delivery['order_delivered_customer_date'] - df_delivery['order_purchase_timestamp']).dt.days
        df_delivery['is_delayed'] = df_delivery['order_delivered_customer_date'] > df_delivery['order_estimated_delivery_date']
        df_delivery['delay_status'] = df_delivery['is_delayed'].map({True: '지연 배송', False: '정시 배송'})

        def bucket_delivery(days):
            if days <= 3: return '0-3일'
            elif days <= 7: return '4-7일'
            elif days <= 14: return '8-14일'
            else: return '15일 이상'
        
        df_delivery['delivery_bucket'] = df_delivery['delivery_days'].apply(bucket_delivery)
        df_del_rev = pd.merge(df_delivery, order_reviews, on='order_id')
        del_rev_agg = df_del_rev.groupby(['delivery_bucket', 'delay_status'])['review_score'].mean().reset_index()
        
        fig1 = px.bar(del_rev_agg, x='delivery_bucket', y='review_score', color='delay_status',
                    barmode='group', category_orders={"delivery_bucket": ['0-3일', '4-7일', '8-14일', '15일 이상']},
                    color_discrete_map={'정시 배송': '#2ecc71', '지연 배송': '#e74c3c'},
                    title="배송 소요일 및 지연 여부에 따른 고객 만족도")
        st.plotly_chart(fig1, use_container_width=True)

    with m_tab2:
        st.subheader("상품 카테고리별 주문 취소율")
        order_prod = pd.merge(order_items, products[['product_id', 'product_category_name']], on='product_id')
        order_prod_trans = pd.merge(order_prod, translation, on='product_category_name', how='left')
        if search_query:
            order_prod_trans = order_prod_trans[order_prod_trans['product_category_name_english'].str.contains(search_query, case=False, na=False)]
        order_status_df = pd.merge(order_prod_trans, orders[['order_id', 'order_status']], on='order_id')
        cat_stats = order_status_df.groupby('product_category_name_english')['order_status'].value_counts(normalize=True).unstack().fillna(0)
        if 'canceled' in cat_stats.columns:
            cat_cancel = cat_stats['canceled'].sort_values(ascending=False).head(20).reset_index()
            fig2 = px.bar(cat_cancel, x='canceled', y='product_category_name_english', orientation='h', title="상위 20개 카테고리별 주문 취소율")
            st.plotly_chart(fig2, use_container_width=True)
        else: st.info("취소된 주문이 없습니다.")

    with m_tab3:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("결제 수단별 평균 주문 금액")
            pay_avg = payments.groupby('payment_type')['payment_value'].mean().reset_index()
            st.plotly_chart(px.bar(pay_avg, x='payment_type', y='payment_value', color='payment_type'), use_container_width=True)
        with col2:
            st.subheader("할부 횟수에 따른 평균 결제 금액")
            inst_avg = payments[payments['payment_installments'] > 0].groupby('payment_installments')['payment_value'].mean().reset_index()
            st.plotly_chart(px.line(inst_avg, x='payment_installments', y='payment_value', markers=True), use_container_width=True)

    with m_tab4:
        st.subheader("브라질 주(State)별 매출 현황")
        cust_orders = pd.merge(orders[['order_id', 'customer_id']], customers[['customer_id', 'customer_state']], on='customer_id')
        order_rev = pd.merge(cust_orders, payments.groupby('order_id')['payment_value'].sum().reset_index(), on='order_id')
        state_revenue = order_rev.groupby('customer_state')['payment_value'].sum().reset_index()
        st.plotly_chart(px.bar(state_revenue.sort_values('payment_value', ascending=False), x='customer_state', y='payment_value', color='payment_value'), use_container_width=True)

    with m_tab5:
        st.header("💡 심층 인사이트")
        st.subheader("1. 리뷰 점수가 재구매율에 미치는 영향")
        # 실제 재구매 여부 분석 로직
        user_orders = orders.merge(customers[['customer_id', 'customer_unique_id']], on='customer_id')
        repurchase = user_orders.groupby('customer_unique_id')['order_id'].nunique().reset_index()
        repurchase['is_repurchase'] = repurchase['order_id'] > 1
        rev_rep = pd.merge(pd.merge(orders[['order_id', 'customer_id']], order_reviews[['order_id', 'review_score']], on='order_id'),
                          customers[['customer_id', 'customer_unique_id']], on='customer_id')
        rev_rep = pd.merge(rev_rep, repurchase[['customer_unique_id', 'is_repurchase']], on='customer_unique_id')
        rev_impact = rev_rep.groupby('review_score')['is_repurchase'].mean().reset_index()
        st.plotly_chart(px.line(rev_impact, x='review_score', y='is_repurchase', markers=True, title="리뷰 점수별 재구매율"), use_container_width=True)

    with m_tab6:
        st.header("📈 네이버 트렌드 비교")
        keywords_str = st.text_input("비교 키워드 (쉼표 구분)", "의류, 가전, 뷰티")
        if st.button("트렌드 조회"):
            kws = [k.strip() for k in keywords_str.split(',')]
            res = fetch_naver_trend(kws, "2023-01-01", "2024-01-01")
            if res:
                all_data = []
                for group in res['results']:
                    for entry in group['data']:
                        all_data.append({'period': entry['period'], 'ratio': entry['ratio'], 'keyword': group['title']})
                st.plotly_chart(px.line(pd.DataFrame(all_data), x='period', y='ratio', color='keyword'), use_container_width=True)

else:
    # --- OLIST-한국 비교 모드 ---
    st.sidebar.header("🇰🇷 비교 분석 필터")
    comparison_theme = st.sidebar.selectbox("비교 주제 선택", [
        "1. 물류 거점 및 배송 효율성",
        "2. 지역 경제력과 소비 패턴",
        "3. 전자상거래 실태 및 결제",
        "4. 판매자 신뢰도 및 성과",
        "5. 소비자 만족도 및 행동"
    ])
    
    available_states = sorted(customers['customer_state'].unique().tolist())
    default_selection = [s for s in ['SP', 'RJ', 'MG'] if s in available_states]
    if not default_selection and available_states: default_selection = [available_states[0]]
    region_filter = st.sidebar.multiselect("브라질 지역(주) 필터", available_states, default=default_selection)
    
    available_years = sorted(orders['order_purchase_timestamp'].dt.year.unique().tolist(), reverse=True)
    year_filter = st.sidebar.selectbox("분석 대상 연도", available_years, index=0)

    st.title("🇰🇷 OLIST-한국 비교 분석 리포트")
    
    # 필터링 적용 (비교용)
    f_orders = orders[orders['order_purchase_timestamp'].dt.year == year_filter].copy()
    f_orders = pd.merge(f_orders, customers, on='customer_id', how='inner')
    if region_filter:
        f_orders = f_orders[f_orders['customer_state'].isin(region_filter)]
    
    # KPI
    col1, col2, col3, col4 = st.columns(4)
    with col1: st.metric("총 주문수", f"{len(f_orders):,}")
    with col2:
        m_rev = pd.merge(f_orders, order_reviews, on='order_id', how='inner')
        st.metric("평균 평점", f"{m_rev['review_score'].mean():.2f}" if not m_rev.empty else "0.00")
    with col3:
        o_pay = payments.groupby('order_id')['payment_value'].sum().reset_index()
        m_pay = pd.merge(f_orders, o_pay, on='order_id', how='inner')
        st.metric("총 매출액", f"R$ {m_pay['payment_value'].sum():,.0f}")
    with col4: st.metric("대상 연도", f"{year_filter}")

    st.markdown("---")

    if comparison_theme == "1. 물류 거점 및 배송 효율성":
        st.subheader("🚚 양국 지역별 배송 효율성 비교")
        c1, c2 = st.columns(2)
        with c1:
            st.write("🇧🇷 브라질 주별 평균 배송일수")
            df_del_days = f_orders.dropna(subset=['order_delivered_customer_date']).copy()
            df_del_days['delivery_days'] = (df_del_days['order_delivered_customer_date'] - df_del_days['order_purchase_timestamp']).dt.days
            st.plotly_chart(px.bar(df_del_days.groupby('customer_state')['delivery_days'].mean().reset_index().sort_values('delivery_days'), 
                                 x='customer_state', y='delivery_days', color='delivery_days', color_continuous_scale='Reds'), use_container_width=True)
        with c2:
            st.write("🇰🇷 한국 시도별 물동량 (가상)")
            st.plotly_chart(px.bar(kr_delivery, x='시도', y='물동량', color='평균배송시간'), use_container_width=True)
        st.success("**🔍 데이터 해석 및 인사이트**\n* 브라질은 영토가 넓어 주별 격차가 크며, 한국은 집적도가 높아 전국이 일일 배송권에 가깝습니다.")

    elif comparison_theme == "2. 지역 경제력과 소비 패턴":
        st.subheader("💰 경제력 지표와 소비 패턴")
        o_pay = payments.groupby('order_id')['payment_value'].sum().reset_index()
        rev_state = pd.merge(f_orders, o_pay, on='order_id')
        st.plotly_chart(px.pie(rev_state.groupby('customer_state')['payment_value'].sum().reset_index().sort_values('payment_value', ascending=False).head(10), 
                             values='payment_value', names='customer_state', title="Brazil Top 10 Sales States"), use_container_width=True)
        st.success("**🔍 데이터 해석 및 인사이트**\n* 브라질과 한국 모두 경제 중심지에 매출의 50% 이상이 집중되는 공통된 소비 집중 현상을 보입니다.")

    elif comparison_theme == "3. 전자상거래 실태 및 결제":
        st.subheader("💳 물가(CPI) 추이와 매출 상관성 분석")
        monthly = f_orders.copy()
        monthly['month'] = monthly['order_purchase_timestamp'].dt.to_period('M').astype(str)
        o_pay = payments.groupby('order_id')['payment_value'].sum().reset_index()
        monthly = pd.merge(monthly, o_pay, on='order_id')
        br_m = monthly.groupby('month')['payment_value'].sum().reset_index()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=br_m['month'], y=br_m['payment_value'], name='OLIST Sales', line=dict(color='blue')))
        fig.add_trace(go.Scatter(x=kr_economy['month'], y=kr_economy['cpi'], name='Korea CPI', yaxis='y2', line=dict(color='red')))
        fig.update_layout(yaxis2=dict(overlaying='y', side='right'), title="매출 vs 물가지수 추이")
        st.plotly_chart(fig, use_container_width=True)
        st.success("**🔍 데이터 해석 및 인사이트**\n* 브라질은 할부(Installments)가, 한국은 간편결제가 구매 전환의 주요 동인입니다.")

    elif comparison_theme == "4. 판매자 신뢰도 및 성과":
        st.subheader("⭐ 판매 성과와 서비스 품질")
        s_perf = pd.merge(order_items, order_reviews, on='order_id')
        s_avg = s_perf.groupby('seller_id').agg({'review_score': 'mean', 'order_id': 'count'}).reset_index()
        st.plotly_chart(px.scatter(s_avg[s_avg['order_id']>10].head(50), x='order_id', y='review_score', size='order_id'), use_container_width=True)
        st.success("**🔍 데이터 해석 및 인사이트**\n* 높은 리뷰 평점은 장기적으로 판매자의 생존율과 매출 가속화에 결정적인 영향을 미칩니다.")

    elif comparison_theme == "5. 소비자 만족도 및 행동":
        st.subheader("📉 배송 지연과 고객 만족도 상관관계")
        c_l, c_r = st.columns(2)
        with c_l:
            d_rev = pd.merge(orders, order_reviews, on='order_id')
            d_rev['delay'] = (d_rev['order_delivered_customer_date'] - d_rev['order_estimated_delivery_date']).dt.days.fillna(0)
            st.plotly_chart(px.scatter(d_rev.sample(1000), x='delay', y='review_score', trendline="ols", title="Delay vs Score"), use_container_width=True)
        with c_r:
            st.plotly_chart(px.pie(kr_complaints, values='count', names='type', title="Korea Complaints"), use_container_width=True)
        st.success("**🔍 데이터 해석 및 인사이트**\n* 브라질은 배송 지연에 민감하며, 한국은 배송 속도보다 상품 파손 등에 더 민감한 차이를 보입니다.")
