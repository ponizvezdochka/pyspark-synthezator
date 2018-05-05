import re
import datetime
import decimal
import pandas as pd
import numpy as np

class Synthezator():

    def __init__(self, seed=None):
        self.alphabet = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
        self.timestamp = datetime.datetime.now()
        if seed:
            np.random.seed(seed)

    def generate_seq(self, size, alphabet=None):
        return ''.join(np.random.choice(list(alphabet), replace=True, size=size)) if alphabet else\
            ''.join(np.random.choice(list(self.alphabet), replace=True, size=size))

    def generate_synthetic(self, col, col_type):
        try:
            if col_type == 'string':
                return '[' + col + ']' + self.generate_seq(size=np.random.randint(1, 10))
            elif 'char' in col_type:
                format_ = re.findall('(?:var)*char\(([\d]*)\)', col_type.replace(' ', ''))[0]
                return self.generate_seq(size=int(format_))
            elif col_type == 'timestamp':
                return self.timestamp
            elif col_type == 'date':
                return self.timestamp.date()
            elif 'decimal' in col_type or 'numeric' in col_type:
                format_ = re.search('(?:decimal|numeric)+\(([\d,]*)\)', col_type.replace(' ', '')).groups()[0]
                after_comma = int(format_.split(',')[1])
                before_comma = int(format_.split(',')[0]) - after_comma
                number_ = str(np.random.randint(1, 10))
                return decimal.Decimal('.'.join([number_*before_comma, number_*after_comma]))
            elif col_type == 'tinyint':
                return np.random.randint(-128, 128)
            elif col_type == 'smallint':
                return np.random.randint(-32768, 32768)
            elif col_type in ['integer', 'int', 'bigint', 'float', 'double']:
                return np.random.randint(-2147483648, 2147483648)
            elif col_type == 'boolean':
                return np.random.choice(['true', 'false'])
            elif col_type == 'binary':
                return int(self.generate_seq(size=np.random.randint(1, 10), alphabet='01'))
            else:
                print('Unknown datatype: %s' % col_type)
        except:
            print('Unknown datatype: %s' % col_type)

    def generate_row(self, columns, formats):
        assert len(columns) == len(formats)
        new_row = {}
        for col, col_type in zip(columns, formats):
            new_row[col] = self.generate_synthetic(col, col_type)
        return new_row

    def generate_table(self, columns, formats, n_rows):
        df = []
        for i in range(n_rows):
            df.append(self.generate_row(columns, formats))
        return pd.DataFrame(df, columns=columns, dtype=object)
