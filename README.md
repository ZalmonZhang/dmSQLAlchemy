# dmSQLAlchemy

该仓库主要提供了支持通过SQLAlchemy连接达梦数据库的方言包

# 主要功能

支持SQLAlchemy的基本功能在SQLAlchemy连接达梦数据库中的实现，支持的功能包括表的映射、类型的映射、对于数据库内进行操作、对于返回信息的展示等，支持SQLAlchemy各个对应版本

# 使用方法

### 1、安装
源码安装：
  在setup.py文件夹下，在较高的pip版本支持下，可以通过 ``` pip install . ``` 进行安装，如果提示报错，请通过 ``` python setup.py install ``` 进行安装
使用pip命令安装：
  可以通过 ``` pip install dmSQLAlchemy ``` 进行下载安装，需要注意的是由于SQLAlchemy有版本区分，如果当前版本不适配，请指定版本下载，如 ```pip install dmSQLAlchemy==1.4.39 ``` 进行下载1.4.39版本的dmSQLAlchemy，具体的dmSQLAlchemy与SQLAlchemy版本对应关系请参照ChangeLogs.md文件描述

### 2、使用
直接通过使用SQLAlchemy连接达梦数据库可以直接使用dmSQLAlchemy
例如：
```
from sqlalchemy import create_engine, text, Column, Integer, String, Identity, insert
from sqlalchemy.orm import declarative_base, Session

engine = create_engine("dm+dmPython://SYSDBA:******@localhost:5236/")
Base=declarative_base()
class A(Base):
    __tablename__ = "test_a"
    id = Column(Integer, Identity(), primary_key=True)
    count = Column(Integer, default=5)
    data = Column(String(20))

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

insert_data=[{"data":"abc"}, {"data":"def"}]

sess=Session(engine)

ids = sess.scalars(insert(A).returning(A.id - A.count), insert_data).all()

```
