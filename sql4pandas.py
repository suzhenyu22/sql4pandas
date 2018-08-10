"""
将SQL翻译成pandas的操作
SQL格式(类MySQL)：

update table_1 a
   set a.name=b.name,
       a.age=a.score+3,
       a.level=my_func(b.level, a.id)
  left join table_2 b    -- 使用join 或left join关联
    on a.id=b.id         -- 翻译成 pandas_merge 的 left_on 和 right_on
 where a.age>3              -- 支持比较操作
   and b.score<=3           -- 支持比较操作
   and a.score>b.score      -- 支持两边对比操作
   and a.id is in (1,2,3)   -- 支持 isin 操作
   and b.id not in (1,2,3)  -- 支持 not in 操作
   and a.score <> 100       -- 支持不等于操作
   and a.id is not null     -- 支持 not null
   and b.level is null      -- 支持 is null
支持操作：
1、带where条件的单表update
2、带where条件的两个表update
3、自定义python函数

不支持的操作：
1、like，比如 string like '%abc%
4、SQL中对字段进行字符串操作，比如 replace(a.name,'小明','小红')

基本假设：
1、SQL中没有对 '\n' 的操作，比如替换，因为在解析SQL时会将换行删除
2、只支持2个表的join操作，不支持3个及以上表关联
3、不对字符字段进行操作，即操作涉及到的列没有字符型，但是支持 =,!=,isin, not in
4、不支持case when 等复杂语句


"""
import re
import sqlparse
import pandas as pd


def judge_format(sql, table_1_cols, table_2_cols):
    """sql校验"""
    # 去掉换行符,多个空格只保留1个
    sql = re.compile(r'\s{2,100}', flags=re.I | re.S).subn(' ', sql.replace('\n', ' '))[0].strip()
    # 判断是否有表别名
    # update table_1 a set xxx
    tb1 = re.compile(r"update (.+?) (.+?) set", flags=re.I | re.S).findall(sql)
    if len(tb1) == 0:
        raise Exception('update表没有表别名，请检查')
    # left join table_2 b on
    if ' left join ' in sql and ' on ' in sql:
        tb2 = re.compile(r" left join (.+?) (.+?) on", flags=re.I | re.S).findall(sql)
        if len(tb2) == 0:
            raise Exception('join表没有表别名，请检查')
    elif ' join ' in sql and ' on ' in sql:
        tb2 = re.compile(r" join (.+?) (.+?) on", flags=re.I | re.S).findall(sql)
        if len(tb2) == 0:
            raise Exception('join表没有表别名，请检查')
    # 不应该有as
    if ' as ' in sql:
        raise Exception('表别名不需要as关键字，请删除')
    # 没有where的不需要SQL
    if ' where ' not in sql:
        raise Exception('没有where的sql用pandas就好了，不需要SQL这么折腾')
    # 判断是否有大写字母
    for s in sql:
        if s.isupper():
            raise Exception('SQL应该是全小写的，但是你传进来的SQL出现了大写字母')
    # 检查字段名是否带表别名
    sql_p = list(sqlparse.parse(sql)[0].flatten())
    for i in range(1, len(sql_p), 1):
        first = sql_p[i - 1]
        second = sql_p[i]
        if second._get_repr_name()=='Name' and second.value.strip() in table_1_cols and first.value!='.':
            raise Exception('字段名没有表别名，请检查，写法应该是：table_1_别名.%s'%second.value.strip())
        if second._get_repr_name()=='Name' and second.value.strip() in table_2_cols and first.value!='.':
            raise Exception('字段名没有表别名，请检查，写法应该是：table_2_别名.%s' % second.value.strip())
    return


def get_all_columns(sql):
    """解析获取所有涉及的字段名"""
    sql_p = list(sqlparse.parse(sql)[0].flatten())
    all_cols = []
    for i in range(2, len(sql_p), 1):
        first = sql_p[i - 2]
        second = sql_p[i - 1]
        third = sql_p[i]
        if first._get_repr_name() == 'Name' \
                and second._get_repr_name() == 'Punctuation' \
                and second.value == '.' \
                and third._get_repr_name() == 'Name':
            all_cols.append('%s.%s' % (first.value.strip(), third.value.strip()))
    return set(all_cols)


def tb_condition_cols_parser(sql):
    """
    从SQL中提取表名，别名，条件，涉及到的字段.
    locals是上层模块的globals，包含所有的对象，是字典类型
    """
    # 去掉换行符,多个空格只保留1个
    sql = re.compile(r'\s{2,100}', flags=re.I | re.S).subn(' ', sql.replace('\n', ' '))[0].strip()
    # 取update的表名和表别名
    tb1 = re.compile(r"update (.+?) (.+?)set", flags=re.I | re.S).findall(sql)[0]
    tb1, tb1_a = tb1[0].strip(), tb1[1].strip()
    # 提取join的表名和表别名
    if ' left join ' in sql:
        tb2 = re.compile(r"left join (.+?) (.+?)on", flags=re.I | re.S).findall(sql)[0]
        tb2, tb2_a = tb2[0].strip(), tb2[1].strip()
        join_type = 'left'
    elif ' join ' in sql:
        tb2 = re.compile(r"join (.+?) (.+?)on", flags=re.I | re.S).findall(sql)[0]
        tb2, tb2_a = tb2[0].strip(), tb2[1].strip()
        join_type = 'inner'
    else:
        tb2, tb2_a = '', ''
        join_type = ''
    # 提取set的内容
    if ' join ' not in sql:
        set_sql = re.compile(r" set(.+?) where ", flags=re.I | re.S).findall(sql)[0].strip()
    elif ' left join ' in sql:
        set_sql = re.compile(r" set(.+?) left join ", flags=re.I | re.S).findall(sql)[0].strip()
    elif ' join ' in sql:
        set_sql = re.compile(r" set(.+?) join ", flags=re.I | re.S).findall(sql)[0].strip()
    else:
        set_sql = ''
    # on 内容
    if ' join ' in sql:
        on_sql = re.compile(r" on(.+?) where ", flags=re.I | re.S).findall(sql)[0].strip()
    else:
        on_sql = ''
    # 提取where条件
    where_sql = re.compile(r" where(.*)", flags=re.I | re.S).findall(sql)[0].strip()
    # list(sqlparse.parse(where_sql)[0].get_sublists())
    # 提取a表涉及的所有列
    all_cols = get_all_columns(sql)
    tb1_cols = [col for col in all_cols if col.startswith(tb1_a + '.')]
    tb2_cols = [col for col in all_cols if col.startswith(tb2_a + '.')]
    # 组装成一个字典返回
    sql_info = {'tb1': tb1, 'tb1_a': tb1_a,
                'tb2': tb2, 'tb2_a': tb2_a,
                'join_type': join_type,
                'on_sql': on_sql,
                'set_sql': set_sql, 'where_sql': where_sql,
                'tb1_cols': tb1_cols, 'tb2_cols': tb2_cols}
    return sql_info


def tb1_join_tb2_parser(sql_info):
    """将涉及到的tb2字段全部join到tb1中，用左联接"""
    # 如果没有table2，返回空
    if not sql_info['tb2']:
        return {}
    # 取出带别名的tb1,tb2字段名
    tb1_cols = sql_info['tb1_cols']
    tb2_cols = sql_info['tb2_cols']

    # list的任意一个元素是否在string中的函数
    def list_in_string(string, list):
        for item in list:
            if item in string:
                return item

    # 找到两个表的关联字段
    left_on = []
    right_on = []
    for on_sql_sub in sql_info['on_sql'].split(' and '):
        # item=sql_info['on_sql'].split(' and ')[0]
        left_on.append(list_in_string(on_sql_sub, tb1_cols))
        right_on.append(list_in_string(on_sql_sub, tb2_cols))
    join_parser = {
        'left_on': left_on,
        'right_on': right_on
    }
    return join_parser


def tb1_filter_and_update_parser(sql_info):
    """
    前面已经将tb2 join 到 tb1 中，下面就是执行 where 和 set 操作了
    """
    # 取出set和where语句
    set_sql = sql_info['set_sql']
    where_sql = sql_info['where_sql']
    # 接下来就是转换 where_sql，使之符合pandas的语法
    where_sql_t = ''
    sub = ''
    for item in sqlparse.parse(where_sql)[0].flatten():
        item = item.value
        # 一个等号改2个等号
        if item == '=':
            sub += '=='
        elif item not in ('and', 'or'):  # 如果是断句，就应该小心了
            sub += item
        elif item in ('and', 'or'):
            # 处理and or断句
            if ' not in ' in sub:
                sub = '-' + sub
            else:
                sub = sub
            where_sql_t += sub
            where_sql_t += item
            sub = ''
    # 最后一个也放进去然后转回去
    if ' not in ' in sub:
        sub = ' -' + sub
    where_sql_t += sub
    where_sql = where_sql_t
    # in和not in，注意替换的顺序
    where_sql = where_sql.replace(' not in ', '.isin').replace(' in ', '.isin')
    # 在isin外面在嵌套一层括号
    where_sql_t = ''
    isin = False
    left_kh = False
    right_kh = False
    for item in sqlparse.parse(where_sql)[0].flatten():
        item = item.value
        if item == 'isin':
            isin = True
        if item == '(' and isin == True:
            item = '(('
            left_kh = True  # 已经提取左括号，标记
        elif item == ')' and isin == True and left_kh == True:
            item = '))'
            isin = False  # 将isin标记
            left_kh = False  # 将左括号标记
        where_sql_t += item
    # and or 转 & |
    where_sql_t = where_sql_t.replace(' and ', ') & (').replace(' or ', ') | (')
    where_sql = '(' + where_sql_t + ')'
    # null 和 notnull
    if ' is null ' in where_sql:
        where_sql = where_sql.replace(' is null ', '.isnull()')
    if ' is not null ' in where_sql:
        where_sql = where_sql.replace(' is not null ', '.notnull()')
    # <> 转 !=
    where_sql_t = ''
    for item in sqlparse.parse(where_sql)[0].flatten():
        if item._get_repr_name() == 'Comparison' and item.value.strip() == '<>':
            where_sql_t += '!='
        else:
            where_sql_t += item.value.strip()
    where_sql = where_sql_t
    # 将 a.id 写成 df['id'] 的格式
    tb1_cols = sql_info['tb1_cols']  # tb1涉及到的字段名，包含表别名
    tb2_cols = sql_info['tb2_cols']
    tb1, tb1_a = sql_info['tb1'], sql_info['tb1_a']
    for col in tb1_cols:
        col1 = col  # 此时已经包含表别名和字段名
        col2 = "%s['%s']" % (tb1, col.split('.')[-1])
        where_sql = where_sql.replace(col1, col2)
    # 需要注意的是，tb2的字段名需要增加 t_tmp_ 前缀标志
    # 注意这里是tb1，因为tb2已经join到tb1了
    for col in tb2_cols:
        col1 = col  # 此时已经包含表别名和字段名
        col2 = "%s['b_tmp_%s']" % (tb1, col.split('.')[-1])
        where_sql = where_sql.replace(col1, col2)
    # 对于set_sql要麻烦一些，要先分割，然后在替换
    # set_sql='a.age=b.b_tmp_age2+2, a.score=func(a.age, b.b_tmp_level2)'
    # set_sql = 'a.age=b.b_tmp_age2+2'
    set_sql_t = sqlparse.parse(set_sql)[0].tokens[0]
    if set_sql_t._get_repr_name() == 'Comparison':
        set_sql = [set_sql_t.value]
    if set_sql_t._get_repr_name() == 'IdentifierList':
        set_sql = [t.value for t in set_sql_t.get_sublists()]
    # 表名替换
    set_sqls2 = []
    for set_t in set_sql:
        for col in tb1_cols:
            col1 = col
            col2 = "%s.loc[wz, '%s']" % (tb1, col.split('.')[-1])
            set_t = set_t.replace(col1, col2)
        for col in tb2_cols:
            col1 = col
            # 注意，这里tb用tb1，col是tb2的，而且要加上 b_tmp_ 前缀
            col2 = "%s.loc[wz, 'b_tmp_%s']" % (tb1, col.split('.')[-1])
            set_t = set_t.replace(col1, col2)
        set_sqls2.append(set_t)
    set_sqls = set_sqls2
    #
    update_parser = {
        'where_sql': where_sql,
        'set_sqls': set_sqls  # 是一个列表
    }
    return update_parser


def update_tb2_column_name_then_join(sql_info, join_parser, table_1, table_2):
    """更新B表字段名, 然后A表关联B表，对关联后的结果校验"""
    # 重命名B表所有涉及的列
    tb2_cols_rename = []  # 其他涉及到的，不是关联key字段也要重命名
    for col in sql_info['tb2_cols']:
        col1 = col.split('.')[-1]
        col2 = 'b_tmp_' + col1
        if col1 not in table_2.columns:
            raise Exception('在%s中找不到%s字段' % (sql_info['tb2'], col1))
        table_2.rename(columns={col1: col2}, inplace=True)
        tb2_cols_rename.append(col2)
    # 重命名join on的列
    tb1_join_col = [col.split('.')[-1] for col in join_parser['left_on']]  # tb1去掉表名
    tb2_join_col = ['b_tmp_%s' % col.split('.')[-1] for col in join_parser['right_on']]  # tb2去表名再加上tmp标志
    # 关联
    len1 = len(table_1)
    table_1 = pd.merge(left=table_1,
                       right=table_2[tb2_cols_rename],
                       left_on=tb1_join_col,
                       right_on=tb2_join_col,
                       how=sql_info['join_type'])
    # 检查前后数据量变化
    len2 = len(table_1)
    print('注意，关联后数据量变化 %d --> %d' % (len1, len2)) if len1 != len2 else None
    # 检查数据类型的变化
    for col in [col for col in table_2.columns if col.startswith('b_tmp_')]:
        dtype_new = table_1.dtypes[col]
        dtype_old = table_2.dtypes[col]
        if dtype_old != dtype_new:
            print('注意，左右表关联后，原来的列 %s 的数据类型 %s -- > %s' % (col, str(dtype_old), str(dtype_new)))
    # 重命名B表所有涉及的列回原来的列名
    for col in tb2_cols_rename:
        col1 = col
        col2 = col.replace('b_tmp_', '')
        table_2.rename(columns={col1: col2}, inplace=True)
    # 最后返回
    return table_1


def update_tb1(sql_info, update_parser, table_1, func1, func2, func3):
    """根据update_parser更新table1"""
    exec('wz = ' + update_parser['where_sql'])
    for set_sql in update_parser['set_sqls']:
        exec(set_sql)
    return table_1


def delete_tb1_that_tmp_column(table_1):
    # 删除以b_tmp_开头的临时列
    for col in table_1.columns:
        if col.startswith('b_tmp_'):
            del table_1[col]
    return table_1


def sql_update_pd(sql, table_1=None, table_2=None, func1=None, func2=None, func3=None):
    """主函数"""
    # 第一步，判断SQL是否正确
    table_1_cols = table_1.columns.tolist()
    table_2_cols = table_2.columns.tolist() if isinstance(table_2, pd.DataFrame) else []
    judge_format(sql, table_1_cols, table_2_cols)
    # 第二步，解析SQL，同时再次判断SQL语法是否正确
    sql_info = tb_condition_cols_parser(sql)
    # 第三步，join关联解析
    join_parser = tb1_join_tb2_parser(sql_info)
    # 第四步，update解析
    update_parser = tb1_filter_and_update_parser(sql_info)
    # 第五步，更新B表的字段名
    # 第六步，A join B
    # 第七步，将B表的列名改回原来的
    if sql_info['tb2']:
        table_1 = update_tb2_column_name_then_join(sql_info, join_parser, table_1=table_1, table_2=table_2)
    # 第八步，update A
    table_1 = update_tb1(sql_info, update_parser, table_1 = table_1, func1=func1, func2=func2, func3=func3)
    # 第9步，删除A中临时列
    table_1 = delete_tb1_that_tmp_column(table_1 = table_1)
    # 第10步，返回A
    return table_1


def test():
    # 构造测试数据
    df1 = pd.DataFrame(data={'id': [1, 2, 3, 4], 'age': [2, 3, 4, 5], 'score': [3, 4, 7, 6], 'heigh': [5, 6, 8, 7]})
    df2 = pd.DataFrame(data={'id': [1, 2, 3, 4], 'age2': [3, 4, 5, 6], 'score2': [5, 6, 7, 7]})
    print(df1)
    print(df2)

    # 测试语法是否正确
    sql = """ update table_1 set age=id+1 """  # 没有表别名
    sql = """ update table_1 as a set age=id+1 """  # 不应该用as
    sql = """ update table_1 a set a.age = a.id + a.score"""  # 没有where条件，就没必要搞SQL这么麻烦了
    judge_format(sql)

    # 正常是写法，字段名也应该带表别名
    sql = """ update table_1 a set a.age = a.id + a.score where a.heigh>=5"""
    judge_format(sql)

    # ------------------------------------------------------------
    # 单表更新
    sql = "update table_1 a set a.age=a.id*2 where a.id>=3"
    sql_update_pd(sql, table_1 = df1)
    # ------------------------------------------------------------
    # 联合更新
    sql = "update table_1 a " \
          "set a.age=a.id+b.age2+1 " \
          "left join table_2 b " \
          "on a.id=b.id " \
          "where a.age>2 and b.age2<6"  # 更新中间两行
    new_df_1 = sql_update_pd(sql, table_1=df1, table_2=df2)

    # ------------------------------------------------------------
    # 使用自定义python函数
    def add(a, b):
        return a * b
    def add2(a,b):
        return a-b

    df1['t']=0
    sql = "update table_1 a set a.score=add(a.id, a.age), a.t = add2(a.age, a.heigh) where a.id>=3"
    new_df_1 = sql_update_pd(sql, table_1=df1, func1=add, func2=add2)

    # ------------------------------------------------------------
    # 多字段更新
    sql = "update table_1 a set a.score=a.id+2, a.heigh=a.age+2 where a.id>=3"
    new_df_1 = sql_update_pd(sql, table_1=df1)
