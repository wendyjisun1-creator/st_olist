import pandas as pd
import os

# 데이터 폴더 경로
data_dir = r'c:\fcicb6\data\OLIST\DATA_1'
# 원본 파일명 리스트 (확장자 제외)
target_files = [
    'olist_customers_dataset',
    'olist_orders_dataset',
    'olist_order_items_dataset',
    'olist_order_reviews_dataset',
    'olist_products_dataset',
    'olist_order_payments_dataset',
    'olist_sellers_dataset',
    'product_category_name_translation',
    'olist_geolocation_dataset'
]

print("📦 데이터 변환 및 압축 시작 (CSV -> Parquet)...")
for base_name in target_files:
    csv_path = os.path.join(data_dir, base_name + '.csv')
    parquet_path = os.path.join(data_dir, base_name + '.parquet')
    
    if os.path.exists(csv_path):
        try:
            # 데이터 로드
            df = pd.read_csv(csv_path)
            # Parquet로 저장 (압축률 매우 높음)
            df.to_parquet(parquet_path, index=False, engine='pyarrow')
            
            csv_size = os.path.getsize(csv_path) / (1024*1024)
            pq_size = os.path.getsize(parquet_path) / (1024*1024)
            print(f"✅ {base_name}.csv: {csv_size:.1f}MB -> {pq_size:.1f}MB 변환 완료")
        except Exception as e:
            print(f"❌ {base_name}.csv 변환 실패: {e}")
    else:
        print(f"⚠️ {base_name}.csv 파일이 존재하지 않습니다. 건너뜁니다.")

print("\n🚀 변환이 완료되었습니다. 이제 DATA_1 폴더 내의 .parquet 파일들을 GitHub에 업로드하세요!")
