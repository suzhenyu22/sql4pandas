
# 背景
Python Data Analysis Library 或 pandas 是基于NumPy 的一种工具，该工具是为了解决数据分析任务而创建的。Pandas 纳入了大量库和一些标准的数据模型，提供了高效地操作大型数据集所需的工具。pandas提供了大量能使我们快速便捷地处理数据的函数和方法。你很快就会发现，它是使Python成为强大而高效的数据分析环境的重要因素之一。

在工作中经常操作数据表会有这样一种感受，在某些具体的统计分析等场景下是pandas比较高效，但是在快速迭代的业务场景下，pandas的代码重构和数据分析就显得繁琐了，而且代码的可读性没有SQL好。基于这样的需求，萌生了将SQL和pandas相结合的想法，即用写SQL代码，将SQL转成pandas的语法然后执行得到结果。如果直接用数据库作为存储用SQL重构，不仅工作量大，而且有些业务逻辑的函数是SQL无法实现的，这也是为什么要以pandas为基础，经SQL转成pandas执行，而不是反过来，将pandas dataframe导入到数据库用SQL重构。

# How about using sql on pandas
In the work of operating the data table often has such a feeling, in some specific statistical analysis and other scenarios is pandas more efficient, but in a fast iterative business scenario, pandas code reconstruction and data analysis becomes tedious , And the readability of the code is not as good as SQL. Based on such requirements, the idea of combining SQL and pandas was initiated. That is, by writing SQL code, converting SQL to pandas's syntax and executing the result. If you directly use the database as a storage for SQL refactoring, not only the workload is large, but also some functions of the business logic are SQL can not be achieved, which is why it is necessary to use pandas as a basis to perform the conversion from SQL to pandas instead of vice versa. The pandas dataframe is imported into the database and reconstructed using SQL.

SQL与pandas语法哪个简洁，我们看如下比较，功能都是实现子集的操作：
SQL and pandas syntax which is concise, we see the following comparison, the function is to achieve a subset of the operation:
```sql 
-- sql 
update table1 set age=score+levels where id>=5
-- sql 
update table1 a set a.age=b.age+2 join table2 b on a.id=b.id where a.age>2 and b.age<10

```
```Python
# pandas 
table1.loc[table1['id']>5,'age']=table1.loc[table1['id']>5,'score']+table1.loc[table1['id']>5,'levels]
# pandas 
table2.rename(columns={'id':'id2','age':age2'}, inplace=True)
table1=pd.merge(left=table1, right=table2[['id2','age2']], left_on='id',right_on='id2', how='inner')
loc=table1['age']>2 & table1['age2']<10
table1.loc[loc, 'age']=table1.loc['age2']+2
```

感受下上面实现同样的功能，但是代码量完全不同，特别是第二种情况，明显是SQL可读性更强，也更容易维护，在面对快速变化的业务需求，SQL的优势就显示出来了，毕竟业务工程化不需要很复杂的统计函数，更多是增删改查。
基于上面的直观感受，是我们开发SQL on pandas的原因。

Feel the same functions as above, but the amount of code is completely different. Especially in the second case, obviously SQL is more readable and easier to maintain. In the face of rapidly changing business requirements, the advantages of SQL are displayed. After all, business engineering does not require complicated statistical functions.
Based on the above intuitive feelings, we are the reason for the development of SQL on pandas.

# 现状
目前github上已经有类似开源项目，比如
[sql4pandas](https://github.com/keeganmccallum/sql4pandas) 基于sqlite3
[sandals](https://github.com/jbochi/sandals) 没有update
[framequery ](https://github.com/chmp/framequery) 不支持标准sql
[sqlpandas](https://github.com/jvlingam/sqlpandas) 只支持简单select
[PandasSQL](https://github.com/naveenrc/PandasSQL) 基于sqlite3

在开源项目中，做得比较好的是sql4pandas，实现大部分SQL语法，查看其源码后发现，其实就是将SQL中涉及到的dataframe导入到sqlite3中，然后执行标准sqlite的SQL。为了加快速度，sqlite3是存储在内存中，没有磁盘IO。
sandals更进一步，没有使用sqlite3做存储，而是将SQL语法直接转换成pandas语法，但是sandals仅支持select操作，不支持update。

之所以没有选用先用开源项目的原因，是因为现在的开源项目并不满足我们的需求，sql4pandas虽然支持update操作，但是其本质是在执行标准92SQL，有些自定义的python函数不能使用，同时我们也测试过，将1百万行100列的dataframe导入sqlite3再将其导出，时间大约是2分钟，效率并不高。同理PandasSQL也是一样。
我们在项目中的大致思路是，先将所有数据整合到一个表中，然后使用update更新数据，这样不仅更好理解业务逻辑，而且更好组织代码。

我们希望，能用最少的代码实现单表更新，和多表联合更新，能支持自定义python函数，至于其他的需求，其实并不是那么重要。如果能满足这三种用得最多的功能，将大大提高生产力，以支持快速迭代的业务.
```sql
-- 单表更新
update table1 set age=score+2 where id>4

-- 多表联合更新
update table1 a set a.age=b.age join table2 b on a.id=b.id where a.id>4

-- 支持python自定义函数
update table1 set age = py_define_func(score, levels)
```
因此我们开发了这样一个转换模块，将SQL转换成pandas语法，以更少的代码完成用pandas完成update操作，其原理是，用正则表达式和SQL解析引擎配合，将SQL转成pandas代码执行。

Therefore, we have developed a conversion module that converts SQL to pandas syntax and uses pandas to complete the update operation with less code. The principle is to use regular expressions and the SQL parsing engine to convert SQL to pandas code execution.

# 实现过程
## 基本假设和约定
  - 1.SQL格式。类似sqlserver和sybaseIQ的语法，但不是标准的SQL，为了在解析多字段同时更新而特别做的限定，同时考虑如果以后改用数据库做存储，代码重构工作量减少到最小。
  
  Similar to the sqlserver and sybaseIQ syntax, but not the standard SQL, in order to parse multiple fields at the same time to update the special restrictions, while considering that if you later use the database for storage, code reconstruction workload is reduced to a minimum.
  
  
```sql
update table_1 a
   set a.name=b.name,
       a.age=a.score+3,
       a.level=my_func(b.level, a.id)
  left join table_2 b       -- 使用join 或left join关联
    on a.id=b.id            -- 翻译成 pandas_merge 的 left_on 和 right_on
 where a.age>3              -- 支持比较操作
   and b.score<=3           -- 支持比较操作
   and a.score>b.score      -- 支持两边对比操作
   and a.id is in (1,2,3)   -- 支持 isin 操作
   and b.id not in (1,2,3)  -- 支持 not in 操作
   and a.score <> 100       -- 支持不等于操作
   and a.id is not null     -- 支持 not null
   and b.level is null      -- 支持 is null
```

  - 2.SQL中不能有'\n'的操作，比如字符串中换行符的替换，因为在自定义函数中会将SQL换行删除。因此实现约定不能有字符型数据，如果只是简单更新可以。
  - 3.只支持2个表的join操作，不支持3个及以上表关联
  - 4.字符串列只支持简单的比较操作，如 =,!=, in, not in 
  - 5.update操作涉及到的列数据类型均为数值型
  - 6.所有表必须有表别名，字段前必须带表别名
  - 7.不支持as表别名操作
  - 8.不支持case when等复杂SQL，可以用自定义python函数替代

## 实现方法
将SQL转成pandas，核心是通过正则表达式和SQL解析引擎解析格式固定的SQL语句，然后执行字符串相关操作，得到最终的pandas语法的过程。
核心的正则表达式有：

1、提取update表名和别名的正则表达式:

Extract regular expressions for update table names and aliases

```python
import re 
sql="update table_1 a set a.age=a.id+b.age2+1 join table_2 b on a.id=b.id where a.age>2 and b.age2<6"
re.compile(r"update (.+?) (.+?)set", flags=re.I | re.S).findall(sql)[0].strip()
```

2、提取join表名和别名的正则表达式：

Extract regular expressions for join table names and aliases

```python
import re 
sql="update table_1 a set a.age=a.id+b.age2+1 join table_2 b on a.id=b.id where a.age>2 and b.age2<6"
re.compile(r"join (.+?) (.+?)on on", flags=re.I | re.S).findall(sql)[0].strip()
re.compile(r"left join (.+?) (.+?)on on", flags=re.I | re.S).findall(sql)[0].strip()
```
3、提取set语句的正则表达式

Extract the regular expression of the set statement

```python
import re 
sql="update table_1 a set a.age=a.id+b.age2+1 from table_2 b where a.id=b.id and a.age>2 and b.age2<6"
re.compile(r"set(.+?) join", flags=re.I | re.S).findall(sql)[0].strip()
re.compile(r"set(.+?) left join", flags=re.I | re.S).findall(sql)[0].strip()
```

4、提取where条件的正则表达式

Extract regular expressions for where conditions

```python
import re 
sql="update table_1 a set a.age=a.id+b.age2+1 from table_2 b where a.id=b.id and a.age>2 and b.age2<6"
re.compile(r"where(.*)", flags=re.I | re.S).findall(sql)[0].strip()
```

转换操作：
在上一步中我们已经提取了正则子串，接下来就是对这些正则子串的操作，此时需要用sqlparse解析得到更精确的结果，才能进行做更精细的控制。
主要是where条件和set的操作,操作前后的效果如下：

In the previous step we have extracted the regular substrings. The next step is the operation of these regular substrings. In this case, you need to use sqlparse to get more accurate results in order to do more fine-grained control.
Mainly the conditions of the where and set operations, the effect before and after the operation are as follows:

```python
# 原SQL
sql="update table_1 a set a.age=a.id+b.age2+1 from table_2 b where a.id=b.id and a.age>2 and b.age2<6"

# set操作转换后
# wz 即 where 条件筛选出来的行索引
table_1.loc[wz, 'age']=table_1.loc[wz, 'id']+table_1.loc[wz, 'b_tmp_age2']+1

# where条件转换后
(table_1['id']==table_1['b_tmp_id']) & (table_1['age']>2) & (table_1['b_tmp_age2']<6)
```

## globals()的使用
在项目中，为了减少代码量和保持代码的整洁，使用了globals()作为参数传入API中。globals和locals，顾名思义，globals是全局的，locals是局部的，它们是python两个内置的函数，locals() 和globals()，它们提供了基于字典的访问局部和全局变量的方式。
在一个Python程序中的任何一个地方，都存在几个可用的名字空间。每个函数都有着自已的名字空间，叫做局部名字空间，它记录了函数的变量，包括 函数的参数和局部定义的变量。每个模块拥有它自已的名字空间，叫做全局名字空间，它记录了模块的变量，包括函数、类、其它导入的模块、模块级的变量和常 量。还有就是内置名字空间，任何模块均可访问它，它存放着内置的函数和异常。
当一行代码要使用变量 x 的值时，Python会到所有可用的名字空间去查找变量，按照如下顺序：

1、局部名字空间 - 特指当前函数或类的方法。如果函数定义了一个局部变量 x，Python将使用这个变量，然后停止搜索。
2、全局名字空间 - 特指当前的模块。如果模块定义了一个名为 x 的变量，函数或类，Python将使用这个变量然后停止搜索。
3、内置名字空间 - 对每个模块都是全局的。作为最后的尝试，Python将假设 x 是内置函数或变量。

使用globals()，可以一次性将全部变量传入函数内，简化引用方法。在`sql4pandas`项目中同样使用了globals()的方法，其原理就是，在globals()中找dataframe，找到了就将其导入到sqlite3中。

## exec的使用
在项目中我们使用了exec函数执行SQL on pandas，因为传进来的是SQL字符串，因为执行字符表达式的最方便的选择。
exec()和eval()函数很相似，都是计算字符表达式的值，区别是：

exec()动态执行python代码，也就是说exec可以执行复杂的python代码，而不像eval函数那样只能计算一个表达式的值。

eval()计算指定表达式的值，也就是说它要执行的python代码只能是单个表达式（注意eval不支持任何形式的赋值操作），而不能是复杂的代码逻辑。


# 使用方法
下面自定义两个dataframe测试使用方法，其中`sql_update_pd`是封装好后的API，直接调用即可

There are some examples:

```python 
    # 构造测试数据
    table_1 = pd.DataFrame(data={'id': [1, 2, 3, 4], 'age': [2, 3, 4, 5], 'score': [3, 4, 7, 6], 'heigh': [5, 6, 8, 7]})
    table_2 = pd.DataFrame(data={'id': [1, 2, 3, 4], 'age2': [3, 4, 5, 6], 'score2': [5, 6, 7, 7]})
    print(table_1)
    print(table_2)

    # 测试语法是否正确
    sql = """ update table_1 set age=id+1 """  # 没有表别名(there should be an alias table name)
    sql = """ update table_1 as a set age=id+1 """  # 不应该用as(you should not use 'as')
    sql = """ update table_1 a set a.age = a.id + a.score"""  # 没有where条件，就没必要搞SQL这么麻烦了(if not where, pandas is more easy than sql)
    judge_format(sql)

    # 正常是写法，字段名也应该带表别名
    sql = """ update table_1 a set a.age = a.id + a.score where a.heigh>=5"""
    judge_format(sql)

    # ------------------------------------------------------------
    # 单表更新(update table)
    sql = "update table_1 a set a.age=a.id*2 where a.id>=3"
    table_1 = sql_update_pd(sql, g_objects=locals())
    # ------------------------------------------------------------
    # 联合更新(update table from another table )
    sql = "update table_1 a set a.age=a.id+b.age2+1 left join table_2 b on a.id=b.id where a.age>2 and b.age2<6"  # 更新中间两行
    table_1 = sql_update_pd(sql, g_objects=locals())

    # ------------------------------------------------------------
    # 使用自定义python函数(use python define function in sql)
    def add(a, b):
        return a + b

    sql = "update table_1 a set a.score=add(a.id, a.age) where a.id>=3"
    table_1 = sql_update_pd(sql, g_objects=locals())

    # ------------------------------------------------------------
    # 多字段更新(update two or more columns onece)
    sql = "update table_1 a set a.score=a.id+2 set a.heigh=a.age+2 where a.id>=3"
    table_1 = sql_update_pd(sql, g_objects=locals())

```

以上就是 sql on pandas 的主要思路和方法，欢迎大家下载试用，提出更好的意见。

振裕
2018-5-25

