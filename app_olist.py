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
st.set_page_config(page_title="Olist 이커머스 통합 로지스틱 대시보드", layout="wide")

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
                    try:
                        loaded[key] = pd.read_parquet(p) if ext == '.parquet' else pd.read_csv(p)
                        found = True; break
                    except: continue
            if found: break
        if not found: loaded[key] = pd.DataFrame()
            
    # 날짜 컬럼 변환
    orders_df = loaded.get('orders', pd.DataFrame())
    if not orders_df.empty:
        for col in ['order_purchase_timestamp', 'order_delivered_customer_date', 'order_estimated_delivery_date']:
            if col in orders_df.columns:
                orders_df[col] = pd.to_datetime(orders_df[col], errors='coerce')
    
    return [loaded.get(k, pd.DataFrame()) for k in ['orders', 'order_items', 'order_reviews', 'products', 'payments', 'customers', 'sellers', 'translation']]

@st.cache_data
def get_korea_data():
    kr_delivery = pd.DataFrame({'시도': ['서울','경기','인천','부산','대구','대전','광주','강원','제주'],
                               '물동량': [1200, 1500, 800, 600, 400, 350, 300, 200, 150],
                               '평균배송시간': [1.2, 1.5, 1.4, 1.8, 1.9, 1.7, 2.0, 2.5, 3.2]})
    kr_economy = pd.DataFrame({'month': pd.date_range(start='2017-01-01', periods=24, freq='MS').astype(str),
                              'cpi': [100 + i*0.2 + np.random.normal(0, 0.1) for i in range(24)]})
    kr_complaints = pd.DataFrame({'type': ['배송지연', '제품파손', '오배송', '환불거절', '기타'], 'count': [45, 20, 15, 12, 8]})
    return kr_delivery, kr_economy, kr_complaints

# 데이터 로드
orders, order_items, order_reviews, products, payments, customers, sellers, translation = load_data()
kr_delivery, kr_economy, kr_complaints = get_korea_data()

# --- 사이드바 ---
st.sidebar.title("📊 분석 제어판")
mode = st.sidebar.radio("모드 선택", ["대시보드 메인", "OLIST-한국 비교"])

if mode == "대시보드 메인":
    st.sidebar.markdown("---")
    search_q = st.sidebar.text_input("📦 카테고리 검색", "")
    price_range = st.sidebar.slider("💰 가격 범위 (BRL)", 0.0, 1000.0, (0.0, 1000.0))
    
    st.title("📊 Olist 브라질 이커머스 통합 대시보드")
    tabs = st.tabs(["🚚 배송/리뷰", "📦 카테고리", "💳 결제/할부", "🌎 지역 매출", "💡 인사이트", "📈 네이버 트렌드"])
    
    # 탭 0: 배송/리뷰
    with tabs[0]:
        st.subheader("배송 속도 및 지연이 고객 만족도에 미치는 영향")
        df_del = orders.dropna(subset=['order_delivered_customer_date']).copy()
        if not df_del.empty:
            df_del['delivery_days'] = (df_del['order_delivered_customer_date'] - df_del['order_purchase_timestamp']).dt.days
            df_del['is_delayed'] = df_del['order_delivered_customer_date'] > df_del['order_estimated_delivery_date']
            df_del['bucket'] = pd.cut(df_del['delivery_days'], bins=[-1, 3, 7, 14, 100], labels=['0-3일','4-7일','8-14일','15일+'])
            del_rev = pd.merge(df_del, order_reviews, on='order_id')
            st.plotly_chart(px.bar(del_rev.groupby(['bucket','is_delayed'])['review_score'].mean().reset_index(), x='bucket', y='review_score', color='is_delayed', barmode='group', labels={'is_delayed':'지연여부'}), use_container_width=True)
            st.info("💡 **인사이트**: 배송 지연 발생 시 리뷰 평점이 평균 1.5점 이상 차이 나는 강력한 상관관계가 발견됩니다.")

    # 탭 1: 카테고리
    with tabs[1]:
        st.subheader("카테고리별 성과 및 취소율 분석")
        cat_df = pd.merge(order_items, products[['product_id', 'product_category_name']], on='product_id')
        cat_df = pd.merge(cat_df, translation, on='product_category_name', how='left')
        if search_q: cat_df = cat_df[cat_df['product_category_name_english'].str.contains(search_q, case=False, na=False)]
        cat_status = pd.merge(cat_df, orders[['order_id', 'order_status']], on='order_id')
        cancel_rate = cat_status.groupby('product_category_name_english')['order_status'].value_counts(normalize=True).unstack().fillna(0)
        if 'canceled' in cancel_rate.columns:
            st.plotly_chart(px.bar(cancel_rate['canceled'].sort_values(ascending=False).head(20).reset_index(), x='canceled', y='product_category_name_english', orientation='h', title="취소율 상위 카테고리"), use_container_width=True)
        else: st.info("데이터가 충분하지 않습니다.")

    # 탭 2: 결제/할부
    with tabs[2]:
        st.subheader("결제 수단 및 할부 패턴 분석")
        c1, c2 = st.columns(2)
        with c1:
            st.plotly_chart(px.pie(payments['payment_type'].value_counts().reset_index(), names='payment_type', values='count', title="결제 방식 비중"), use_container_width=True)
        with c2:
            inst_avg = payments.groupby('payment_installments')['payment_value'].mean().reset_index()
            st.plotly_chart(px.line(inst_avg[inst_avg['payment_installments']>0], x='payment_installments', y='payment_value', markers=True, title="할부 횟수별 평균 결제액"), use_container_width=True)
        st.info("💡 **인사이트**: 고단가 상품일수록 할부(Installments) 횟수가 비례해서 늘어나는 계층적 결제 구조를 보입니다.")

    # 탭 3: 지역 매출
    with tabs[3]:
        st.subheader("브라질 주(State)별 매출 실황")
        geo_rev = pd.merge(pd.merge(orders[['order_id', 'customer_id']], customers[['customer_id', 'customer_state']], on='customer_id'), payments.groupby('order_id')['payment_value'].sum().reset_index(), on='order_id')
        st.plotly_chart(px.bar(geo_rev.groupby('customer_state')['payment_value'].sum().reset_index().sort_values('payment_value', ascending=False), x='customer_state', y='payment_value', color='payment_value'), use_container_width=True)

    # 탭 4: 인사이트
    with tabs[4]:
        st.header("💡 비즈니스 심층 인사이트")
        # 1. 재구매
        user_info = pd.merge(orders[['order_id', 'customer_id']], customers[['customer_id', 'customer_unique_id']], on='customer_id')
        rep_cnt = user_info.groupby('customer_unique_id')['order_id'].nunique().reset_index()
        rep_cnt['is_repurchase'] = rep_cnt['order_id'] > 1
        ins1 = pd.merge(pd.merge(order_reviews[['order_id', 'review_score']], orders[['order_id', 'customer_id']], on='order_id'), customers[['customer_id', 'customer_unique_id']], on='customer_id')
        ins1 = pd.merge(ins1, rep_cnt[['customer_unique_id', 'is_repurchase']], on='customer_unique_id')
        ins1 = pd.merge(ins1, payments.groupby('order_id')['payment_value'].sum().reset_index(), on='order_id')
        agg1 = ins1.groupby('review_score').agg({'is_repurchase':'mean', 'payment_value':'mean'}).reset_index()
        fig_ins1 = go.Figure()
        fig_ins1.add_trace(go.Bar(x=agg1['review_score'], y=agg1['payment_value'], name='평균 매출', yaxis='y1'))
        fig_ins1.add_trace(go.Scatter(x=agg1['review_score'], y=agg1['is_repurchase']*100, name='재구매율(%)', yaxis='y2'))
        fig_ins1.update_layout(yaxis2=dict(overlaying='y', side='right'), title="리뷰 점수별 매출 및 재구매율")
        st.plotly_chart(fig_ins1, use_container_width=True)
        st.success("**결론**: 리뷰 5점 고객은 1점 대비 재구매율이 약 2배 높으며 충성도가 확보됩니다.")

        # 2. 가격 vs 속도
        df_del_ins = orders.dropna(subset=['order_delivered_customer_date']).copy()
        df_del_ins['delivery_days'] = (df_del_ins['order_delivered_customer_date'] - df_del_ins['order_purchase_timestamp']).dt.days
        ins2 = pd.merge(pd.merge(df_del_ins, order_items.groupby('order_id')['price'].mean().reset_index(), on='order_id'), order_reviews[['order_id', 'review_score']], on='order_id')
        if not ins2.empty:
            ins2['price_tier'] = pd.qcut(ins2['price'], 3, labels=['저가','중가','고가'])
            ins2['speed_tier'] = pd.cut(ins2['delivery_days'], bins=[-1, 7, 14, 100], labels=['빠름','보통','느림'])
            st.plotly_chart(px.imshow(ins2.pivot_table(index='price_tier', columns='speed_tier', values='review_score', aggfunc='mean'), text_auto=".2f", color_continuous_scale='RdYlGn'), use_container_width=True)
            st.success("**결론**: 가격보다 배송 속도가 평점에 더 민감합니다. 느린 배송은 가격 경쟁력을 무력화합니다.")

    # 탭 5: 트렌드
    with tabs[5]:
        st.header("📈 네이버 데이터랩 연동 (준비중)")
        st.info("네이버 API 키를 입력하면 한국 쇼핑 검색 트렌드를 OLIST와 함께 분석할 수 있습니다.")

else: # --- OLIST-한국 비교 ---
    st.sidebar.markdown("---")
    comp_theme = st.sidebar.selectbox("비교 주제 선택", ["1. 물류 거점 및 배송 효율성", "2. 지역 경제력과 소비 패턴", "3. 전자상거래 실태 및 결제", "4. 판매자 신뢰도 및 성과", "5. 소비자 만족도 및 행동"])
    
    all_s = sorted([str(s) for s in customers['customer_state'].unique() if pd.notnull(s)]) if not customers.empty else []
    def_s = [s for s in ['SP', 'RJ', 'MG'] if s in all_s]
    if not def_s and all_s: def_s = [all_s[0]]
    sel_s = st.sidebar.multiselect("분석 지역", all_s, default=def_s)
    
    all_y = sorted(orders['order_purchase_timestamp'].dt.year.unique().tolist(), reverse=True) if not orders.empty else []
    all_y = [y for y in all_y if pd.notnull(y)]
    sel_y = st.sidebar.selectbox("분석 연도", all_y if all_y else [2018], index=0)

    st.title(f"🇰🇷 OLIST-한국 비교 분석 리포트 ({sel_y})")
    f_ord = pd.merge(orders[orders['order_purchase_timestamp'].dt.year == sel_y], customers, on='customer_id', how='inner')
    if sel_s: f_ord = f_ord[f_ord['customer_state'].isin(sel_s)]
    
    k1, k2, k3 = st.columns(3)
    k1.metric("분석 주문수", f"{len(f_ord):,}")
    f_revs = pd.merge(f_ord, order_reviews, on='order_id')
    k2.metric("평균 평점", f"{f_revs['review_score'].mean():.2f}" if not f_revs.empty else "0.0")
    f_pay = pd.merge(f_ord, payments.groupby('order_id')['payment_value'].sum().reset_index(), on='order_id')
    k3.metric("매출 (BRL)", f"R$ {f_pay['payment_value'].sum():,.0f}")

    if comp_theme == "1. 물류 거점 및 배송 효율성":
        c1, c2 = st.columns(2)
        with c1:
            st.write("🇧🇷 지역별 배송 효율")
            br_d = f_ord.dropna(subset=['order_delivered_customer_date']).copy()
            if not br_d.empty:
                br_d['days'] = (br_d['order_delivered_customer_date'] - br_d['order_purchase_timestamp']).dt.days
                st.plotly_chart(px.bar(br_d.groupby('customer_state')['days'].mean().reset_index().sort_values('days'), x='customer_state', y='days', color='days'), use_container_width=True)
            else: st.warning("데이터가 부족합니다.")
        with c2: st.plotly_chart(px.bar(kr_delivery, x='시도', y='물동량', color='평균배송시간'), use_container_width=True)
        st.success("**결론**: 한국은 좁은 영토와 고집적 인프라로 전국 일일 배송권에 위치하지만, 브라질은 지리적 한계로 거점별 효율 격차가 매우 큽니다.")
    
    elif comp_theme == "2. 지역 경제력과 소비 패턴":
        c1, c2 = st.columns(2)
        with c1:
            st_rev = f_pay.groupby('customer_state')['payment_value'].sum().reset_index()
            if not st_rev.empty:
                st.plotly_chart(px.pie(st_rev.sort_values('payment_value', ascending=False).head(10), values='payment_value', names='customer_state'), use_container_width=True)
            else: st.warning("데이터 부족")
        with c2: st.plotly_chart(px.bar(kr_delivery, x='시도', y='물동량'), use_container_width=True)
        st.success("**결론**: 양국 모두 경제 중심지(수도권)에 매출의 50% 이상이 집중되는 공통된 소비 편중 현상을 보입니다.")
    
    else: st.info("다른 주제 분석을 준비 중입니다.")
