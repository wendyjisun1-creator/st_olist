import pandas as pd
import os
import glob

def preprocess_files(directory):
    # CSV와 Parquet 파일 모두 탐색
    files = glob.glob(os.path.join(directory, "*.csv")) + glob.glob(os.path.join(directory, "*.parquet"))
    
    for file_path in files:
        if file_path.endswith("_cleaned.csv") or file_path.endswith("_cleaned.parquet"):
            continue
            
        file_name = os.path.basename(file_path)
        print(f"--- Processing {file_name} ---")
        
        try:
            # 파일 형식에 따라 읽기
            if file_path.endswith(".csv"):
                df = pd.read_csv(file_path)
            else:
                df = pd.read_parquet(file_path)
            
            # 1. 컬럼 이름 공백 제거 및 소문자 변경
            initial_columns = df.columns.tolist()
            df.columns = [str(col).strip().lower() for col in df.columns]
            if initial_columns != df.columns.tolist():
                print(f"  - 컬럼 이름 정리 완료")

            # 2. 중복된 행 확인 및 삭제
            duplicate_count = df.duplicated().sum()
            if duplicate_count > 0:
                df.drop_duplicates(inplace=True)
                print(f"  - 중복된 행 {duplicate_count}개 삭제 완료")
            else:
                print("  - 중복된 행 없음")

            # 3. 결측치 확인 및 평균값 대체 (수치형 컬럼)
            null_counts = df.isnull().sum()
            columns_with_nulls = null_counts[null_counts > 0].index.tolist()
            if columns_with_nulls:
                print(f"  - 결측치가 있는 컬럼: {columns_with_nulls}")
                for col in columns_with_nulls:
                    if pd.api.types.is_numeric_dtype(df[col]):
                        mean_val = df[col].mean()
                        df[col] = df[col].fillna(mean_val)
                        print(f"    * {col}: 평균값({mean_val:.2f})으로 대체")
                    else:
                        print(f"    * {col}: 수치형이 아니므로 평균값 대체 건너뜀")
            else:
                print("  - 결측치 없음")

            # 4. 'date_column'을 날짜 형식으로 변환
            if 'date_column' in df.columns:
                df['date_column'] = pd.to_datetime(df['date_column'], errors='coerce')
                print("  - 'date_column' 날짜 형식으로 변환 완료")
            
            # OLIST 데이터 날짜 컬럼 자동 탐지 변환 (정교한 탐지)
            date_keywords = ['_at', 'date', 'timestamp']
            date_like_cols = [col for col in df.columns if any(kw in col for kw in date_keywords)]
            for col in date_like_cols:
                # 'state'가 포함된 컬럼은 날짜가 아님 (오탐지 방지)
                if 'state' in col:
                    continue
                if not pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    print(f"  - '{col}' 날짜 형식으로 변환 완료 (자동 탐지)")

            # 5. 불필요한 열 삭제 (unnamed 로 시작하는 열 등)
            unnecessary_cols = [col for col in df.columns if 'unnamed' in col]
            if unnecessary_cols:
                df.drop(columns=unnecessary_cols, inplace=True)
                print(f"  - 불필요한 열 삭제: {unnecessary_cols}")

            # 파일 저장
            if file_path.endswith(".csv"):
                output_path = file_path.replace(".csv", "_cleaned.csv")
                df.to_csv(output_path, index=False)
            else:
                output_path = file_path.replace(".parquet", "_cleaned.parquet")
                df.to_parquet(output_path, index=False)
            
            print(f"  - 전처리 완료된 파일 저장: {os.path.basename(output_path)}")
            
        except Exception as e:
            print(f"  - 처리 중 오류 발생: {e}")
        print("\n")

if __name__ == "__main__":
    target_dir = r"c:\fcicb6\data\OLIST\DATA_1"
    preprocess_files(target_dir)
