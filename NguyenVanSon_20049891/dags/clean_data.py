import pandas as pd
import os
import json

def clean_data():
    try:
        df = pd.read_json('/opt/airflow/data/nguyenvanson_stvl.json', lines=True)

        # 'job_name','company_name',update_date,'welfare','job_description'
        # 'LÊ TÂN / NHẬP LIỆU / THIẾT KẾ / PHỤ VIỆC VĂN PHÒNG / GIAO HÀNG / KHO / NHÂN VIÊN IT / TẠP VỤ(Mã:CV2023-26657) ','CÔNG TY TNHH TM MẪN ĐẠT','Ngày cập nhật: 10/04/2023','[' - Nhân viên Lễ tân/ Nhập liệu / Thiết kế / Phụ việc Văn phòng / Nhân viên kho / Nhân viên IT: 6.000.000 – 8.000.000 đ/tháng', ' - Nhân viên Giao hàng: 8.000.000 – 10.000.000 đ/tháng']',''
        # 'Digital Marketing Executive(Mã:CV2023-26805) ','GÀ RÁN PAPA’S CHICKEN (VIỆT AN)','Ngày cập nhật: 22/09/2023','[' ● Mức lương: Cạnh tranh (Deal theo năng lực khi phỏng vấn)', ' ● Môi trường làm việc năng động, có cơ hội học hỏi đào tạo về các công cụ MKT', ' ● Phát triển các kỹ năng: lên kế hoạch, quản trị rủi ro, quản trị đầu việc, giải quyết vấn đề, làm việc nhóm...']','● Phụ trách lập kế hoạch Digital Marketing cho Brand'

        df['update_date'] = pd.to_datetime(df['update_date'], format='Ngày cập nhật: %d/%m/%Y')

        filtered_df = df[df['update_date'].dt.year >= 2023]

        data_dir = f'/opt/airflow/data'
        os.makedirs(data_dir, exist_ok=True)

        filtered_df.to_csv(f'{data_dir}/nguyenvanson_filted.csv', index=False)


    except Exception as e:
        print(e)
