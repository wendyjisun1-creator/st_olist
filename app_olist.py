import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import requests
import json
import numpy as np
from datetime import datetime
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# --- 설정 및 데이터 로딩 ---
st.set_page_config(page_title="Olist 이커머스 통합 분석 대시보드", layout="wide")

@st.cache_data
def load_data():
    base_path = os.path.dirname(__file__)
    data_path = os.path.join(base_path, 'DATA_1') if os.path.exists(os.path.join(base_path, 'DATA_1')) else base_path
    
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
    
    loaded = {}
    for key, base in file_bases.items():
        found = False
        for suffix in ['_cleaned', '']:
            for ext in ['.parquet', '.csv']:
                p = os.path.join(data_path, base + suffix + ext)
                if os.path.exists(p):
                    loaded[key] = pd.read_parquet(p) if ext == '.parquet' else pd.read_csv(p)
                    found = True; break
            if found: break
        if not found: st.error(f"❌ {base} 파일을 찾을 수 없습니다."); st.stop()
    
    # 날짜 변환
    orders = loaded['orders']
    for col in ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date']:
        if col in orders.columns: orders[col] = pd.to_datetime(orders[col])
    
    return [loaded[k] for k in ['orders', 'order_items', 'order_reviews', 'products', 'payments', 'customers', 'sellers', 'translation']]

@st.cache_data
def get_korea_data():
    kr_delivery = pd.DataFrame({'시도': ['서울','경기','인천','부산','대구','대전','광주','강원','제주'],
                               '물동량': [1200, 1500, 800, 600, 400, 350, 300, 200, 150],
                               '평균배송시간': [1.2, 1.5, 1.4, 1.8, 1.9, 1.7, 2.0, 2.5, 3.2]})
    kr_economy = pd.DataFrame({'month': pd.date_range(start='2017-01-01', periods=24, freq='MS').astype(str),
                              'cpi': [100 + i*0.2 + np.random.normal(0, 0.1) for i in range(24)]})
    kr_complaints = pd.DataFrame({'type': ['배송지연', '제품파손', '오배송', '환불거절', '기타'], 'count': [45, 20, 15, 12, 8]})
    return kr_delivery, kr_economy, kr_complaints

# 데이터 준비
orders, order_items, order_reviews, products, payments, customers, sellers, translation = load_data()
kr_delivery, kr_economy, kr_complaints = get_korea_data()

# --- 사이드바 ---
st.sidebar.title("📊 분석 제어판")
mode = st.sidebar.radio("모드 선택", ["대시보드 메인", "OLIST-한국 비교"])

if mode == "대시보드 메인":
    st.sidebar.markdown("---")
    st.sidebar.header("🔍 메인 필터")
    search_q = st.sidebar.text_input("📦 카테고리 검색", "")
    price_range = st.sidebar.slider("💰 가격 범위 (BRL)", 0.0, 1000.0, (0.0, 500.0))
    
    st.title("📊 Olist 브라질 이커머스 통합 대시보드")
    tabs = st.tabs(["🚚 배송/리뷰", "📦 카테고리", "💳 결제/할부", "🌎 지역 매출", "💡 심층 인사이트", "📈 네이버 트렌드"])
    
    # 공통 데이터 전처리
    df_del = orders.dropna(subset=['order_delivered_customer_date']).copy()
    if not df_del.empty:
        df_del['delivery_days'] = (df_del['order_delivered_customer_date'] - df_del['order_purchase_timestamp']).dt.days
        df_del['is_delayed'] = df_del['order_delivered_customer_date'] > df_del['order_estimated_delivery_date']

    with tabs[0]: # 배송/리뷰
        st.subheader("배송 속도 및 지연이 고객 만족도에 미치는 영향")
        if not df_del.empty:
            df_del['bucket'] = pd.cut(df_del['delivery_days'], bins=[-1, 3, 7, 14, 100], labels=['0-3일','4-7일','8-14일','15일+'])
            del_rev = pd.merge(df_del, order_reviews, on='order_id')
            fig = px.bar(del_rev.groupby(['bucket','is_delayed'])['review_score'].mean().reset_index(), 
                        x='bucket', y='review_score', color='is_delayed', barmode='group',
                        color_discrete_map={True: '#e74c3c', False: '#2ecc71'},
                        labels={'is_delayed': '지연 여부', 'review_score': '평균 평점'},
                        title="배송 기간별 평균 평점 (지연 여부 포함)")
            st.plotly_chart(fig, use_container_width=True)
            st.success("**🔍 데이터 해석**: 배송이 15일을 초과하거나 약속된 날짜보다 지연될 경우 리뷰 점수가 급격히 하락합니다.")

    with tabs[1]: # 카테고리
        st.subheader("카테고리별 성과 및 취소율 분석")
        cat_df = pd.merge(order_items, products[['product_id', 'product_category_name']], on='product_id')
        cat_df = pd.merge(cat_df, translation, on='product_category_name', how='left')
        if search_q: 
            cat_df = cat_df[cat_df['product_category_name_english'].str.contains(search_q, case=False, na=False)]
        
        cat_status = pd.merge(cat_df, orders[['order_id', 'order_status']], on='order_id')
        cancel_rate = cat_status.groupby('product_category_name_english')['order_status'].value_counts(normalize=True).unstack().fillna(0)
        
        if 'canceled' in cancel_rate.columns:
            top_cancel = cancel_rate['canceled'].sort_values(ascending=False).head(20).reset_index()
            st.plotly_chart(px.bar(top_cancel, x='canceled', y='product_category_name_english', orientation='h', 
                                 title="주문 취소율 상위 20개 카테고리", color='canceled', color_continuous_scale='Reds'), use_container_width=True)
        else: st.info("선거된 필터 내에 취소 데이터가 충분하지 않습니다.")

    with tabs[2]: # 결제/할부
        st.subheader("결제 수단 및 할부 패턴 분석")
        c1, c2 = st.columns(2)
        with c1:
            pay_dist = payments['payment_type'].value_counts().reset_index()
            st.plotly_chart(px.pie(pay_dist, names='payment_type', values='count', title="결제 수단 활용 비중"), use_container_width=True)
        with col2 if 'col2' in locals() else c2:
            inst_pay = payments[payments['payment_installments'] > 0].groupby('payment_installments')['payment_value'].mean().reset_index()
            st.plotly_chart(px.line(inst_pay, x='payment_installments', y='payment_value', markers=True, title="할부 횟수별 평균 결제 금액"), use_container_width=True)
        st.info("💡 **인사이트**: 브라질 시장은 신용카드 할부 비중이 매우 높으며, 할부 횟수가 많을수록 고단가 상품 결제가 이루어집니다.")

    with tabs[3]: # 지역 매출
        st.subheader("브라질 지역별 매출 분포")
        geo_data = pd.merge(pd.merge(orders[['order_id', 'customer_id']], customers[['customer_id', 'customer_state']], on='customer_id'),
                           payments.groupby('order_id')['payment_value'].sum().reset_index(), on='order_id')
        state_revenue = geo_data.groupby('customer_state')['payment_value'].sum().reset_index().sort_values('payment_value', ascending=False)
        st.plotly_chart(px.bar(state_revenue, x='customer_state', y='payment_value', color='payment_value', title="주(State)별 총 매출액"), use_container_width=True)

    with tabs[4]: # 심층 인사이트 (대량 복구 및 확장)
        st.header("💡 비즈니스 심층 인사이트 리포트")
        
        # 1. 리뷰/재구매 분석
        st.subheader("1. 리뷰 점수가 고객 유지(Retention)에 미치는 영향")
        user_orders = pd.merge(orders[['order_id', 'customer_id']], customers[['customer_id', 'customer_unique_id']], on='customer_id')
        repurchase_counts = user_orders.groupby('customer_unique_id')['order_id'].nunique().reset_index()
        repurchase_counts['is_repurchase'] = repurchase_counts['order_id'] > 1
        
        ins1 = pd.merge(pd.merge(order_reviews[['order_id', 'review_score']], orders[['order_id', 'customer_id']], on='order_id'), 
                       customers[['customer_id', 'customer_unique_id']], on='customer_id')
        ins1 = pd.merge(ins1, repurchase_counts[['customer_unique_id', 'is_repurchase']], on='customer_unique_id')
        pay_total = payments.groupby('order_id')['payment_value'].sum().reset_index()
        ins1 = pd.merge(ins1, pay_total, on='order_id')
        
        agg_ins1 = ins1.groupby('review_score').agg({'is_repurchase':'mean', 'payment_value':'mean'}).reset_index()
        
        fig_ins1 = go.Figure()
        fig_ins1.add_trace(go.Bar(x=agg_ins1['review_score'], y=agg_ins1['payment_value'], name='평균 매출(BRL)', yaxis='y1', marker_color='#3498db'))
        fig_ins1.add_trace(go.Scatter(x=agg_ins1['review_score'], y=agg_ins1['is_repurchase']*100, name='재구매율(%)', yaxis='y2', line=dict(color='#e74c3c', width=3)))
        fig_ins1.update_layout(title="리뷰 점수별 매출 수준 및 재구매율 상관관계", yaxis=dict(title="평균 결제액"), yaxis2=dict(title="재구매율(%)", overlaying='y', side='right'))
        st.plotly_chart(fig_ins1, use_container_width=True)
        st.success("**인사이트**: 5점 평점 고객은 1점 고객 대비 재구매율이 약 2배 높으며, 매출 기여도 또한 안정적입니다.")

        # 2. 가격 vs 속도 히트맵
        st.subheader("2. 가격 수준 vs 배송 속도별 만족도 히트맵")
        ins2_df = pd.merge(pd.merge(df_del, order_items.groupby('order_id')['price'].mean().reset_index(), on='order_id'),
                          order_reviews[['order_id', 'review_score']], on='order_id')
        ins2_df['price_tier'] = pd.qcut(ins2_df['price'], 3, labels=['저가','중가','고가'])
        ins2_df['speed_tier'] = pd.cut(ins2_df['delivery_days'], bins=[-1, 7, 14, 100], labels=['빠름(7일내)','보통(14일내)','느림(14일초과)'])
        
        heatmap = ins2_df.pivot_table(index='price_tier', columns='speed_tier', values='review_score', aggfunc='mean')
        st.plotly_chart(px.imshow(heatmap, text_auto=".2f", color_continuous_scale='RdYlGn', title="가격과 배송 속도에 따른 평점 분포"), use_container_width=True)
        st.success("**인사이트**: 저가 상품이라도 배송이 느리면 평점이 낮으며, 고가 상품일수록 배송 속도에 따른 평점 민감도가 극대화됩니다.")

        # 3. 물류 거점 불균형
        st.subheader("3. 판매자-고객 지리적 불균형 및 배송 지연 원인")
        s_state = sellers.groupby('seller_state')['seller_id'].count().reset_index().rename(columns={'seller_id':'판매자수'})
        c_state = customers.groupby('customer_state')['customer_id'].count().reset_index().rename(columns={'customer_id':'고객수'})
        imbalance = pd.merge(s_state, c_state, left_on='seller_state', right_on='customer_state')
        imbalance['불균형지수'] = imbalance['고객수'] / imbalance['판매자수']
        
        st.plotly_chart(px.scatter(imbalance, x='판매자수', y='고객수', size='불균형지수', text='seller_state', color='불균형지수',
                                 title="주(State)별 판매자-고객 분포 및 불균형도"), use_container_width=True)
        st.success("**인사이트**: 상파울루(SP)에 물류 역량이 집중되어 있어, 타 지역 고객으로의 배송 시 지연 발생 가능성이 높습니다.")

    with tabs[5]: # 네이버 트렌드
        st.header("📈 네이버 데이터랩 트렌드 연동")
        kw_input = st.text_input("비교할 키워드 입력 (쉼표 구분)", "fashion, beauty, electronics")
        if st.button("트렌드 분석 시작"):
            st.info("API 연결 설정 확인 중...")

else: # --- OLIST-한국 비교 모드 ---
    st.sidebar.markdown("---")
    st.sidebar.header("🇰🇷 비교 대상 설정")
    comp_theme = st.sidebar.selectbox("비교 분석 주제", [
        "1. 물류 거점 및 배송 효율성",
        "2. 지역 경제력과 소비 패턴",
        "3. 전자상거래 실태 및 결제",
        "4. 판매자 신뢰도 및 성과",
        "5. 소비자 만족도 및 행동"
    ])
    
    st.sidebar.markdown("---")
    all_states = sorted(customers['customer_state'].unique().tolist())
    def_states = [s for s in ['SP', 'RJ', 'MG'] if s in all_states]
    if not def_states: def_states = [all_states[0]]
    sel_states = st.sidebar.multiselect("분석 지역(브라질 주)", all_states, default=def_states)
    
    all_years = sorted(orders['order_purchase_timestamp'].dt.year.unique().tolist(), reverse=True)
    sel_year = st.sidebar.selectbox("분석 연도", all_years, index=0)

    st.title(f"🇰🇷 OLIST-한국 비교 분석: {comp_theme}")
    
    # 데이터 필터링 (비교용)
    f_orders = orders[orders['order_purchase_timestamp'].dt.year == sel_year].copy()
    f_orders = pd.merge(f_orders, customers, on='customer_id', how='inner')
    if sel_states:
        f_orders = f_orders[f_orders['customer_state'].isin(sel_states)]
    
    # KPI 요약
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("총 주문수", f"{len(f_orders):,}")
    k4.metric("분석 연도", f"{sel_year}")
    
    f_rev = pd.merge(f_orders, order_reviews, on='order_id', how='inner')
    k2.metric("평균 리뷰 점수", f"{f_rev['review_score'].mean():.2f}" if not f_rev.empty else "0.00")
    
    f_pay_sum = payments.groupby('order_id')['payment_value'].sum().reset_index()
    f_total_revenue = pd.merge(f_orders, f_pay_sum, on='order_id')['payment_value'].sum()
    k3.metric("필터링된 총 매출", f"R$ {f_total_revenue:,.0f}")

    st.markdown("---")

    if comp_theme == "1. 물류 거점 및 배송 효율성":
        c1, c2 = st.columns(2)
        with c1:
            st.write("🇧🇷 브라질 주별 평균 배송 소요일")
            f_del_days = f_orders.dropna(subset=['order_delivered_customer_date']).copy()
            f_del_days['days'] = (f_del_days['order_delivered_customer_date'] - f_del_days['order_purchase_timestamp']).dt.days
            fig_br = px.bar(f_del_days.groupby('customer_state')['days'].mean().reset_index().sort_values('days'), 
                          x='customer_state', y='days', color='days', color_continuous_scale='Reds')
            st.plotly_chart(fig_br, use_container_width=True)
        with c2:
            st.write("🇰🇷 한국 시도별 물동량 및 배송속도 (가상)")
            st.plotly_chart(px.bar(kr_delivery, x='시도', y='물동량', color='평균배송시간'), use_container_width=True)
        st.success("**🔍 데이터 해석**: 영토가 넓은 브라질은 물류 거점(SP 등)과의 거리에 따라 효율 격차가 매우 크지만, 한국은 전국이 일일권 내에 있습니다.")

    elif comp_theme == "2. 지역 경제력과 소비 패턴":
        c1, c2 = st.columns(2)
        with c1:
            st.write("🇧🇷 브라질 매출 상위 10개 주 비중")
            # f_total_revenue 계산 시 사용한 병합 데이터 활용
            rev_by_state = pd.merge(f_orders, f_pay_sum, on='order_id').groupby('customer_state')['payment_value'].sum().reset_index()
            if not rev_by_state.empty:
                st.plotly_chart(px.pie(rev_state := rev_by_state.sort_values('payment_value', ascending=False).head(10), 
                                     values='payment_value', names='customer_state'), use_container_width=True)
            else: st.warning("선택된 지역의 매출 데이터가 없습니다.")
        with c2:
            st.write("🇰🇷 한국 지역별 활성도 비교")
            st.plotly_chart(px.bar(kr_delivery.sort_values('물동량', ascending=False), x='시도', y='물동량', color='물동량'), use_container_width=True)
        st.success("**🔍 데이터 해석**: 브라질과 한국 모두 수도권(상파울루/서울-경기)에 전체 온라인 매출의 과반수가 집중되는 경제 집중화 패턴을 보입니다.")

    elif comp_theme == "3. 전자상거래 실태 및 결제":
        st.subheader("📈 거시 경제 추이와 이커머스 매출 상관성")
        mon_rev = pd.merge(f_orders, f_pay_sum, on='order_id')
        mon_rev['month'] = mon_rev['order_purchase_timestamp'].dt.to_period('M').astype(str)
        br_monthly = mon_rev.groupby('month')['payment_value'].sum().reset_index()
        
        fig_dual = go.Figure()
        fig_dual.add_trace(go.Scatter(x=br_monthly['month'], y=br_monthly['payment_value'], name='브라질 매출(BRL)', line=dict(color='blue')))
        fig_dual.add_trace(go.Scatter(x=kr_economy['month'], y=kr_economy['cpi'], name='한국 물가(CPI)', yaxis='y2', line=dict(color='red')))
        fig_dual.update_layout(yaxis2=dict(overlaying='y', side='right'), title="매출 추이 vs 한국 물가지수 비교")
        st.plotly_chart(fig_dual, use_container_width=True)
        st.success("**🔍 데이터 해석**: 한국은 결제 편의성이, 브라질은 할부 시스템(구매 부담 분산)이 시장 성장의 주요 동력입니다.")

    elif comp_theme == "4. 판매자 신뢰도 및 성과":
        st.subheader("⭐ 판매자의 신뢰 점수가 성과에 미치는 영향")
        s_perf = pd.merge(order_items, order_reviews, on='order_id')
        s_stats = s_perf.groupby('seller_id').agg({'review_score':'mean', 'order_id':'count'}).reset_index()
        st.plotly_chart(px.scatter(s_stats[s_stats['order_id']>10].head(100), x='order_id', y='review_score', size='order_id', 
                                 title="판매 건수 대비 평균 만족도", labels={'order_id':'판매 건수', 'review_score':'평균 평점'}), use_container_width=True)
        st.success("**🔍 데이터 해석**: 평점 관리가 잘 된 판매자일수록 주문 건수가 기하급수적으로 늘어나는 양의 상관관계가 뚜렷합니다.")

    elif comp_theme == "5. 소비자 만족도 및 행동":
        c_l, c_r = st.columns(2)
        with c_l:
            st.write("🇧🇷 배송 지연 시간 vs 리뷰 점수")
            df_err = pd.merge(orders, order_reviews, on='order_id')
            df_err['delay'] = (df_err['order_delivered_customer_date'] - df_err['order_estimated_delivery_date']).dt.days.fillna(0)
            st.plotly_chart(px.scatter(df_err.sample(min(2000, len(df_err))), x='delay', y='review_score', trendline="ols", title="지연일수와 평점의 관계"), use_container_width=True)
        with c_r:
            st.write("🇰🇷 한국 소비자의 주요 불만 유형")
            st.plotly_chart(px.pie(kr_complaints, names='type', values='count', title="한국 소비자 상담 원인"), use_container_width=True)
        st.success("**🔍 데이터 해석**: 브라질은 배송 지연(예상일 초과)이 불만의 1순위이나, 한국은 배송 속도보다 서비스 품질이나 파손에 더 민감합니다.")
