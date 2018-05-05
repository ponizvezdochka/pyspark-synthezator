import pandas as pd
from generator import Generator


if __name__ == '__main__':

    g = Generator(seed=19)

    n_rows = 30
    ddl = pd.read_csv('ddl.csv', sep=';')
    table_nm = 'S_ORG_EXT'
    columns = ddl.Column.values
    formats = ddl.Format.str.lower().values

    g.create_table(table_nm, columns, formats, n_rows)
    t = g.sqlContext.table(table_nm)
    t.show(3)


