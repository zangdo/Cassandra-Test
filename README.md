# Kiểm chứng tốc độ truy vấn Cassandra bằng MovieLens 25M

## 1. Mục tiêu

So sánh **cùng câu hỏi: lấy tất cả đánh giá của movie_id = 101**, với hai cách lưu dữ liệu:

```sql
-- Không có partition key user_id: phải quét và lọc trên nhiều partition.
SELECT user_id, rating_time, movie_id, rating
FROM ratings_by_user WHERE movie_id = 101 ALLOW FILTERING;

-- Có đúng partition key movie_id: truy cập một partition logic.
SELECT user_id, rating_time, movie_id, rating
FROM ratings_by_movie WHERE movie_id = 101;
```

ALLOW FILTERING chỉ được dùng làm đối chứng để thấy chi phí của schema không phù hợp;
giải pháp cho truy vấn theo phim là bảng ratings_by_movie. Không thêm index vào hai bảng thử nghiệm.

Không so sánh truy vấn một user LIMIT 20 với truy vấn tất cả đánh giá của một phim:
đó là hai yêu cầu khác nhau, số dòng đầu ra khác nhau.

## 2. Chuẩn bị cluster

Mở Docker Desktop. Trong PowerShell, khởi động lại các container đã dừng:

```powershell
docker start cassandra1 cassandra2 cassandra3
docker exec -it cassandra1 nodetool status
```

Đợi đến khi **đủ 3 dòng UN**. Ba container Up chưa đủ để kết luận ba node đã tham gia cluster.
Không cần tạo lại container hoặc thay đổi volume.

Script giả định cấu hình lab: datacenter1, RF=3, không bật xác thực, cổng node 1 là 9042.
Driver chỉ dùng node 1 làm coordinator, vì chỉ cổng này được publish ra Windows.
Coordinator vẫn liên lạc với các replica khác trong Docker. Đây không phải phép đo cân bằng tải nhiều coordinator.

Với heap 512 MB/node đã dùng trong bài, bắt đầu ở 100.000 dòng; thử 500.000 rồi 1.000.000 nếu máy ổn.
Đừng nạp ngay cả 25 triệu dòng. Hai bảng với RF=3 có chi phí lưu trữ/ghi cao hơn một bảng một bản sao;
25 triệu dòng nguồn tương ứng khoảng 150 triệu bản sao dòng giữa hai bảng, chưa tính cấu trúc lưu trữ.

## 3. Lấy dữ liệu

- Trang chính thức: https://grouplens.org/datasets/movielens/25m/
- ZIP chính thức: https://files.grouplens.org/datasets/movielens/ml-25m.zip (khoảng 250 MB).
- Giải nén; chỉ cần file `ml-25m/ratings.csv` cho chương trình này.
- Đặt thư mục `ml-25m` cạnh `benchmark.py` rồi mở PowerShell tại thư mục đó.

Header phải là:

```csv
userId,movieId,rating,timestamp
```

Ánh xạ: userId → user_id; movieId → movie_id; rating → rating;
timestamp → rating_time (bigint, giây Unix, giữ nguyên giá trị).
Đây là thời điểm người dùng đánh giá, khác với timestamp nội bộ phục vụ last-write-wins của Cassandra.

Bộ dữ liệu không được đóng gói kèm mã nguồn này. Đọc điều kiện sử dụng của GroupLens
và ghi nguồn MovieLens/GroupLens trong báo cáo.

## 4. Nạp 100.000 dòng vào cả hai bảng

Bạn đã có uv. Không cần `uv init` hay tự kích hoạt môi trường: uv đọc dependency ngay trong script.

```powershell
uv run --python 3.12 benchmark.py load --csv .\ml-25m\ratings.csv --rows 100000
```

Nếu ratings.csv ở nơi khác, thay đường dẫn, ví dụ `--csv "D:\Data\ml-25m\ratings.csv"`.

Chương trình tạo keyspace mới **ml_bench_100000**, không sửa keyspace bdsp của bài cũ.
Hai bảng có cùng các cột, khác primary key:

```sql
CREATE TABLE ratings_by_user (
  user_id int, rating_time bigint, movie_id int, rating float,
  PRIMARY KEY ((user_id), rating_time, movie_id)
) WITH CLUSTERING ORDER BY (rating_time DESC);

CREATE TABLE ratings_by_movie (
  user_id int, rating_time bigint, movie_id int, rating float,
  PRIMARY KEY ((movie_id), rating_time, user_id)
) WITH CLUSTERING ORDER BY (rating_time DESC);
```

Chương trình dùng prepared INSERT, giới hạn 16 request đồng thời, không tạo BATCH khổng lồ.
Mỗi dòng CSV được ghi vào cả hai bảng. Ghi ở ALL để chỉ coi import thành công khi đủ 3 replica xác nhận.
ALL ở đây phục vụ việc chuẩn bị dữ liệu nhất quán trước phép đo, không phải khuyến nghị cho mọi ứng dụng.
Chỉ chạy bước tiếp theo khi chương trình báo DONE và manifest có complete=true.

Nạp xong, đợi CPU/đĩa bớt bận. Có thể quan sát:

```powershell
docker stats --no-stream cassandra1 cassandra2 cassandra3
docker exec cassandra1 nodetool compactionstats
docker exec cassandra2 nodetool compactionstats
docker exec cassandra3 nodetool compactionstats
```

Các lệnh này là quan sát hoạt động; một lần thấy ít tải không bảo đảm cache/đĩa của các lần đo giống hệt nhau.

## 5. Đo và kiểm chứng

```powershell
uv run --python 3.12 benchmark.py run --manifest .\results\ml_bench_100000_manifest.json --movie 101
```

Mặc định chương trình:

1. Kiểm tra đúng CSV/subset đã nạp bằng SHA-256; lấy các bản ghi mong đợi của phim từ CSV.
2. Chuẩn bị hai query, cùng movie_id, cột trả về, consistency ONE và page size 1000.
3. Chạy khởi động một lần/query, không tính vào thống kê.
4. Đo 8 lần/query, luân phiên bên nào chạy trước, không bật tracing.
5. Dùng `perf_counter()` đo từ lúc gửi đến khi đã lấy **hết mọi trang** kết quả.
6. So sánh toàn bộ nội dung từng kết quả với CSV; thứ tự dòng không ảnh hưởng kiểm tra.
7. In min/median/max; ghi từng lần đo vào CSV và thông tin tổng hợp vào JSON trong `results`.

Đây là thời gian đầu cuối phía client: gồm xử lý server, truyền dữ liệu, driver giải mã và thu thập các dòng.
Không bao gồm đọc CSV, prepare query, sắp xếp để kiểm tra hoặc in kết quả.
Đây là phép đo sau warm-up, không phải phép đo cold cache.

Các nhãn kết quả:

- **filtered_scan**: truy vấn trên ratings_by_user với ALLOW FILTERING.
- **partition_lookup**: truy vấn bằng partition key trên ratings_by_movie.
- **OK**: toàn bộ bản ghi khớp CSV, không chỉ đúng số lượng.
- **ERROR**: truy vấn gặp lỗi/timeout. elapsed_ms là thời gian tới lúc lỗi, không phải latency truy vấn thành công.
- **MISMATCH**: kết quả khác CSV; không được dùng làm bằng chứng so sánh tốc độ hợp lệ.
- **Scan/lookup median ratio**: median(filtered_scan) / median(partition_lookup).
  Lớn hơn 1 nghĩa là lookup nhanh hơn trong phép đo này. Chỉ xuất tỷ lệ khi mọi lượt đều OK.

Chạy lại lệnh run để kiểm tra độ ổn định; kết quả mới có timestamp riêng, không ghi đè kết quả cũ.
Muốn thử phim khác, dùng --movie 1 hoặc một ID được in trong danh sách popular movie IDs lúc import.
Nếu phim không có trong subset, chương trình sẽ dừng và gợi ý các ID có dữ liệu.

## 6. Tăng lượng dữ liệu

```powershell
uv run --python 3.12 benchmark.py load --csv .\ml-25m\ratings.csv --rows 500000
uv run --python 3.12 benchmark.py run --manifest .\results\ml_bench_500000_manifest.json --movie 101

uv run --python 3.12 benchmark.py load --csv .\ml-25m\ratings.csv --rows 1000000
uv run --python 3.12 benchmark.py run --manifest .\results\ml_bench_1000000_manifest.json --movie 101
```

Mỗi mức dùng keyspace riêng, không cộng dồn nhầm các lần nạp. Mỗi bảng của mức đó chứa cùng subset.
Đợi import và hoạt động nền ổn định trước từng lệnh run; không chạy import và benchmark đồng thời.
Không dừng node, đổi heap hay đổi consistency giữa hai query đang so sánh.

CSV gốc được sắp theo userId rồi movieId, nên lấy N dòng đầu tạo subset theo user,
không phải mẫu ngẫu nhiên đại diện toàn bộ MovieLens. Dùng cùng quy tắc cho mọi mức và nêu điều này trong báo cáo.
Khi tăng N, số đánh giá của movie 101 cũng có thể tăng: ghi rõ số dòng trả về ở từng mức.
Đừng suy ra mọi thay đổi latency đều chỉ do số partition; số kết quả, cache, GC và compaction cũng ảnh hưởng.
Các keyspace cũ chiếm dung lượng đĩa: đây là thử nghiệm học tập, không phải benchmark phần cứng được cô lập hoàn toàn.

## 7. Đưa kết quả vào báo cáo

Điền số đo thật, không sử dụng số minh họa:

| Dòng nguồn / mỗi bảng | Movie ID | Số dòng trả về | Median scan (ms) | Median lookup (ms) | Tỷ lệ scan/lookup | Lỗi |
|---:|---:|---:|---:|---:|---:|---|
| 100.000 | 101 | | | | | |
| 500.000 | 101 | | | | | |
| 1.000.000 | 101 | | | | | |

Chụp: nodetool status đủ 3 UN; import DONE; kết quả benchmark có VERIFIED và các median.
Giữ CSV/JSON kết quả để đối chiếu. Ghi cấu hình CPU/RAM Docker, heap/node, phiên bản Cassandra,
RF=3, write ALL khi import, read ONE khi đo, page size, 8 lần đo/query và warm-up.

Nếu scan chậm hơn rõ rệt và kết quả đều khớp, giải thích bằng việc truy vấn không có user_id
nên không định vị được một partition, trong khi bảng theo movie định vị được partition của phim.
Clustering order phục vụ thứ tự các dòng trong partition; lợi thế chính của phép thử này là partition key.
Lookup vẫn phải đọc/truyền mọi đánh giá của phim; không có nghĩa thời gian luôn là hằng số.

Nếu chênh lệch nhỏ, ghi đúng kết quả rồi thử mức dữ liệu lớn hơn hoặc lặp lại sau khi máy ổn định.
Nếu scan timeout, báo số lần lỗi riêng; không bịa thời gian hoàn thành hoặc tính tỷ lệ từ timeout.
Ba container cùng một máy dùng chung CPU/RAM/đĩa: thí nghiệm này minh họa lựa chọn schema,
không đo khả năng scale của ba máy vật lý độc lập.

## 8. Khi gặp lỗi

- **NoHostAvailable / connection refused:** kiểm tra Docker Desktop, đủ 3 UN,
  `docker port cassandra1 9042`, và cổng host 9042 không bị ứng dụng khác chiếm.
- **Unavailable khi import ALL:** chưa đủ 3 replica sẵn sàng; kiểm tra nodetool status.
- **Keyspace/manifest already exists:** dữ liệu đã có. Nếu import hoàn chỉnh, dùng run, không load lại.
  Nếu import bị gián đoạn/lỗi, tạo bộ thử mới, ví dụ:

  ```powershell
  uv run --python 3.12 benchmark.py load --csv .\ml-25m\ratings.csv --rows 100000 --keyspace ml_bench_100k_retry --concurrency 4
  uv run --python 3.12 benchmark.py run --manifest .\results\ml_bench_100k_retry_manifest.json --movie 101
  ```

  Script không tự xóa keyspace cũ và không dùng dữ liệu import dở để đo.
- **Timeout khi scan:** có thể là kết quả của truy vấn quá tốn tài nguyên; giữ bản ghi lỗi.
  --timeout chỉ đổi timeout phía client, không vô hiệu hóa timeout phía Cassandra.
  Thử mức N nhỏ hơn để có phép so sánh hoàn thành ở cả hai phía.
- **Máy quá tải khi import:** dùng --concurrency 4 với keyspace mới và mức N nhỏ hơn.
  Xem `docker logs --tail 100 cassandra1` (tương tự node 2/3) để xác định lỗi thực tế.
- **Di chuyển ratings.csv sau khi import:** thêm --csv "đường dẫn mới" vào lệnh run.

## Mã chính và nguồn tham khảo

`load_data()` tạo schema và nạp dữ liệu; `inspect_csv()` xác định dữ liệu đúng từ CSV;
`measure()` đo toàn bộ kết quả; `run_benchmark()` lặp phép đo, kiểm tra và xuất báo cáo.

Đoạn quan trọng trong measure():

```python
start = time.perf_counter()
rows = list(session.execute(statement))  # lấy hết các trang, không chỉ trang đầu
elapsed_ms = (time.perf_counter() - start) * 1000
valid = sorted(tuple(row) for row in rows) == expected  # ngoài khoảng thời gian đo
```

Mã đã được kiểm tra cú pháp, dependency, đọc CSV và logic đo/kiểm tra bằng dữ liệu thử cục bộ.
Chưa chạy trên cluster Docker của bạn; các số đo chỉ được tạo khi bạn thực hiện lệnh run.

- MovieLens README: https://files.grouplens.org/datasets/movielens/ml-25m-README.html
- Cassandra CQL / ALLOW FILTERING: https://cassandra.apache.org/doc/latest/cassandra/developing/cql/dml.html
- Driver paging: https://github.com/apache/cassandra-python-driver/blob/trunk/docs/query_paging.rst
- Driver package: https://pypi.org/project/cassandra-driver/3.30.1/
