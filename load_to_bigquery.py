from google.cloud import bigquery
import os

# Nếu bạn chưa set biến môi trường GOOGLE_APPLICATION_CREDENTIALS, hãy bỏ comment dòng bên dưới và điền đường dẫn tới file JSON chứa service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"E:\project\elt-bigquery\credentials.json"

def load_csv_to_bigquery(file_path, project_id, dataset_id, table_id):
    # Khởi tạo client BigQuery
    client = bigquery.Client(project=project_id)
    
    # Tạo reference tới bảng đích
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Cấu hình job load dữ liệu
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Bỏ qua dòng đầu tiên vì đó là header
        autodetect=True,      # Tự động nhận diện schema (kiểu dữ liệu các cột)
        
        # Uncomment dòng dưới nếu muốn ghi đè dữ liệu cũ mỗi khi chạy lại script
        # write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        
        # Uncomment dòng dưới nếu muốn thêm dữ liệu vào bảng đã có (append)
        # write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    print(f"Đang tải dữ liệu từ {file_path} lên bảng {table_ref} ...")
    
    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
    
    # Đợi job hoàn thành
    job.result()
    
    # In ra kết quả
    table = client.get_table(table_ref)
    print(f"Đã tải thành công {table.num_rows} dòng và {len(table.schema)} cột vào {table_ref}.")

if __name__ == "__main__":
    # Đường dẫn file CSV của bạn
    FILE_PATH = r"e:\project\elt-bigquery\global_health_data.csv"
    
    # TODO: Thay thế các biến dưới đây bằng thông tin BigQuery của bạn
    PROJECT_ID = "bigqueryanalysis-491104"  # Ví dụ: "my-project-12345"
    DATASET_ID = "health_data"      # Ví dụ: "health_data"
    TABLE_ID = "global_health_data"     # Ví dụ: "health_stats_2024"
    
    load_csv_to_bigquery(FILE_PATH, PROJECT_ID, DATASET_ID, TABLE_ID)
