from pyspark.sql import SparkSession 
from pyspark.sql.functions import col, sum, avg
  
spark = SparkSession.builder.appName("Exercise").getOrCreate() 
 
# 1. Đọc và Hiển Thị Dữ Liệu: Đọc dữ liệu từ một tệp CSV chứa thông tin về các sản phẩm (tên, giá, số 
# lượng) và hiển thị năm dòng đầu tiên của dữ liệu. 
data_path = "./data.csv" 
products = spark.read.csv(data_path, header=True, inferSchema=True) 
products.show(5)

# 2. Lọc và Sắp Xếp Dữ Liệu: Sử dụng PySpark để lọc ra các sản phẩm có giá trên 100 và sau đó sắp xếp 
# chúng theo thứ tự giảm dần của giá. 
filtered_products = products.filter(products["Giá"]>100)
filtered_products.show()
sorted_products = filtered_products.orderBy("Giá", ascending=False)
sorted_products.show()

# 3. Tính Tổng và Trung Bình: Tính tổng và trung bình giá của các sản phẩm sau khi đã lọc theo yêu cầu 
# bài tập số 2. 
filtered_products.select(sum("Giá")).show()
filtered_products.select(avg("Giá")).show()

# 4. Tạo Thêm Cột: Thêm một cột mới vào DataFrame, ví dụ: "Tổng Giá" bằng cách nhân giá và số lượng 
# của mỗi sản phẩm. 
products = products.withColumn("Tổng Giá", col("Giá") * col("Số lượng"))
products.show()

# 5. Nhóm Dữ Liệu: Nhóm sản phẩm theo danh mục và tính tổng số lượng sản phẩm trong mỗi danh 
# mục.
grouped_products = products.groupBy("Danh mục").count()
grouped_products.show()