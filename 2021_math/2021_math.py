import os
import glob
import shutil
import pandas as pd


class DataPreProcessor:
    def __init__(self, data_type):
        self.data_base_path = '附件1：UWB数据集'
        self.data_type = data_type
        self.df = None

    def get_all_data(self):
        """将目录下所有的txt文件去除表头后拼接成一个列表"""
        parent_path = os.path.join(self.data_base_path, self.data_type)
        data = []
        for filename in glob.glob1(parent_path, '*.txt'):
            with open(os.path.join(parent_path, filename), 'r') as f:
                tag_id = filename.split('.')[0]
                data.extend(self._read_one_file(f, tag_id))
        return data

    @staticmethod
    def _read_one_file(f, tag_id):
        f.readline()  # 第一行
        one_file_data = []
        while 1:
            one_group_data = []
            _timestamp = set()  # 如果timestamp不一致，则认为是异常数据
            for _ in range(4):  # 四条数据为一组
                line = f.readline().strip()
                if not line:
                    return one_file_data
                one_group_data.append(line.split(':')[5])
                _timestamp.add(line.split(':')[1])

            if len(_timestamp) == 1:
                one_file_data.append([tag_id] + list(_timestamp) + one_group_data)

    def process_data(self):
        # 加载数据
        self.df = pd.DataFrame(self.get_all_data(), columns=['tag_id', 'timestamp', '0', '1', '2', '3'])
        # 去重
        self.df.drop_duplicates(['tag_id', '0', '1', '2', '3'], inplace=True)

    def save_processed_data(self):
        processed_file_dir = os.path.join(self.data_base_path, f'处理后的{self.data_type}')
        if os.path.exists(processed_file_dir):
            shutil.rmtree(processed_file_dir)
        os.mkdir(processed_file_dir)

        for tag_id in set(self.df['tag_id']):
            filename = os.path.join(processed_file_dir, f'{tag_id}.{self.data_type.strip("数据")}.csv')
            with open(filename, 'w') as f:
                for _, row in self.df[self.df['tag_id'] == tag_id].iterrows():
                    f.write(','.join(row.values[2:]) + '\n')


if __name__ == '__main__':
    dp = DataPreProcessor('正常数据')
    dp.process_data()
    dp.save_processed_data()

    dp = DataPreProcessor('异常数据')
    dp.process_data()
    dp.save_processed_data()