"""Bài 1: Gửi GET, gửi POST JSON và đọc dữ liệu ThingSpeak.
Cài thư viện: python -m pip install requests
Chạy: python thingspeak_bai1.py
Chương trình sẽ gửi hai bản ghi thật khi chạy.
"""

import time
import requests

# Thông tin trong đề. Nếu dùng kênh riêng, thay cả key và Channel ID.
WRITE_API_KEY = "T7H40F0X82VGW7L5"
CHANNEL_ID = "1529099"
READ_API_KEY = ""  # Chỉ điền nếu kênh riêng tư yêu cầu Read API Key.
UPDATE_URL = "https://api.thingspeak.com/update"


def kiem_tra_gui(response):
    response.raise_for_status()
    entry_id = int(response.text.strip())
    if entry_id > 0:
        print(f"Gửi thành công! entry_id = {entry_id}")
    else:
        print("Gửi thất bại. Kiểm tra Write API Key và khoảng cách giữa các lần gửi.")


def gui_get():
    # params đưa dữ liệu vào query string của URL.
    response = requests.get(
        UPDATE_URL,
        params={"api_key": WRITE_API_KEY, "field1": 20, "field2": 33},
        timeout=20,
    )
    kiem_tra_gui(response)


def gui_post():
    # api_key ở URL; field1, field2 ở JSON body.
    # json= tự đặt header Content-Type: application/json.
    response = requests.post(
        UPDATE_URL,
        params={"api_key": WRITE_API_KEY},
        json={"field1": 25, "field2": 60},
        timeout=20,
    )
    kiem_tra_gui(response)


def doc_du_lieu():
    params = {"results": 2}
    if READ_API_KEY:
        params["api_key"] = READ_API_KEY
    response = requests.get(
        f"https://api.thingspeak.com/channels/{CHANNEL_ID}/feeds.json",
        params=params,
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()  # Parse JSON thành đối tượng Python.
    if not isinstance(data, dict) or not isinstance(data.get("feeds"), list):
        print("Không nhận được mảng feeds. Kiểm tra Channel ID và quyền đọc.")
        return
    if not data["feeds"]:
        print("Kênh chưa có bản ghi để hiển thị.")
    for feed in data["feeds"]:
        temperature = feed.get("field1")
        humidity = feed.get("field2")
        temperature = "chưa có dữ liệu" if temperature is None else f"{temperature} °C"
        humidity = "chưa có dữ liệu" if humidity is None else f"{humidity} %"
        print(f"Bản ghi {feed.get('entry_id')}:")
        print(f"  Nhiệt độ: {temperature}")
        print(f"  Độ ẩm: {humidity}")


def main():
    steps = [("1. Gửi bằng GET", gui_get),
             ("2. Gửi bằng POST JSON", gui_post),
             ("3. Đọc hai bản ghi gần nhất", doc_du_lieu)]
    for index, (title, action) in enumerate(steps):
        if index == 1:
            print("\nChờ 16 giây giữa hai lần gửi...")
            time.sleep(16)
        print(f"\n{title}")
        try:
            action()
        except requests.RequestException:
            print("Lỗi kết nối hoặc HTTP. Kiểm tra mạng, địa chỉ API và quyền truy cập.")
        except ValueError:
            print("Phản hồi không đúng định dạng số entry_id hoặc JSON mong đợi.")


if __name__ == "__main__":
    main()
