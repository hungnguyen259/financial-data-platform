Các thành phần trong source code:
    
- SQL: chứa các query sql để đơn giản hóa việc sử dụng: tạo bảng, xóa bảng, xóa dữ liệu
    
- k8s: chứa các config để cài đặt cluster 
    
  + Airflow: config của Airflow
    
  + Spark: config của Spark
    
- Airflow: thư mục sẽ được mount vào các node worker của cluster k8s
    
   + dags: chứa các file:
    
     . DAG: chứa source code của các dag tương ứng
    
     . Spark App
    
       data_to_db_demo.py: dùng để demo và debug
    
       data_to_db.py: file demo đã loại bỏ các lệnh in
    
     . Driver JDBC dành cho postgres
    
    + logs: chứa log của Airflow
    
- guide.docx: Hướng dẫn cài đặt

