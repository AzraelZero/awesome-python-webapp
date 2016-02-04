
# -*- coding: utf-8 -*-

import time
import uuid
import functools
import threading
import logging

engine = None

def next_id(t = None):
    if t is None:
        t = time.time()
    return '%015d%s000' % (int(t*1000),uuid.uuid4().hex)  #生成一个唯一的id，运用当前时间和伪随机码组合而成

def create_engine(user,password,database,host='127.0.0.1',port=3306,**kw):
    import mysql.connector
    global engine
    if engine is not None:
        raise DBError('engine is already initialized.')
    params = dict(user=user,password=password,database=database,host=host,port=port)
    defaults = dict(user_unicode=True,charset='utf8',collation='utf8_general_ci',autocommit=False)
    for k,v in defaults.iteritems():
        params[k] = kw.pop(k,v)     #加入defaults设置到每一用户参数后面
    params.update(kw)   #更新params里的参数
    params['buffered'] = True
    engine = _Engine(lambda : mysql.connector.connect(**params))
    #test connection...
    logging.info('Init mysql engine <%s> ok' % hex(id(engine)))

def with_connection(func):
    @functools.wraps(func)
    def _warpper(*args,**kw):
        with _ConnectionCtx:
            return func(*args,**kw)
    return  _warpper

def with_transaction(func):
    @functools.wraps(func)
    def _warpper(*args,**kw):
        with _TransactionCtx:
            func(*args,**kw)
    return  _warpper



class Dict(dict):
    def __init__(self,names=(),values=(),**kw): #**kw是一个有多个names和values属性组成的字典，init中需要申明names和values
        super(Dict,self).__init__(**kw)   #super是为了避免重复继承dict，这里调用dict的init函数
        for k,v in zip(names,values):    #zip是一个迭代
            self[k]=v

    def __getattr__(self,key):        #__getattr__是点号运算，这里我们定义点号运算
        try:
            return self[key]
        except KeyError:          #try。。except。。except后接的是错误类型，raise触发一种错误，后面接说明
            raise AttributeError(r"'Dict'object has no attribute '%s'"%key)

    def __setattr__(self, key, value):    #__setattr__是赋值属性，这里定义赋值函数
        self[key]=value




class _Engine(object):
    def __int__(self,conect):
        self.connect = conect
    def connection(self):
        return self.connect()



class _LasyConnection(object):
    def __init__(self):
        pass

class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transations = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        self.connection = _LasyConnection()
        self.transations = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

_db_ctx = _DbCtx()

class _LasyConnection(object):
    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            _connection = engine.connect()
            logging.info('[CONNECTION] [OPEN] connection <%s>...'% hex(id(_connection)))
            self.connection = _connection
        return self.connection.cursor

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            _connection = self.connection()
            self.connection = None
            logging.info('[CONNECTION] [CLOSE] connection <%s>...'% hex(id(connection)))
            connection.close()

class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

def connection():
    return _ConnectionCtx()

def transaction():
    return  _TransactionCtx()

class _TransactionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transaction = _db_ctx.transaction + 1
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _db_ctx
        _db_ctx.transaction = _db_ctx.transaction - 1
        try:
            if _db_ctx.transaction == 0:
                if exc_type is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        try:
            _db_ctx.connection.commit()
        except:
            _db_ctx.connection.rollback()
            raise

    def rollback(self):
        global _db_ctx
        _db_ctx.connection.rollback()

@with_connection
def _select(sql,first,*args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?','%s')           #我们想在输入的时候避免%s的复杂，所以用？代替，这里我们需要替换
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql,args)           #数据库的执行语句
        if cursor.description:
            #cursor。description里面存储了执行的参数
            names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchon()   #返回接下来的查询结果
            if not values:
                return None
            return Dict(names,values)
        return [Dict(names,x) for x in cursor.fetchall()]     #返回查询结果的集合
    finally:
        if cursor:
            cursor.close()

def select_one(sql,*args):
    return _select(sql,True,*args)

def select_int(sql,*args):
    d = select_one(sql,True,*args)
    if len(d) != 1:
        raise MultiColumnsError('Expect only one column.')
    return d.values()[0]

@with_connection
def _updata(sql,*args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?','%s')
    logging.info('sql:%s,args:%s' % (sql,args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql,args)
        r = cursor.rowcount
        if _db_ctx.transaction == 0:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()

def updata(sql,*args):
    return _updata(sql,*args)

#@with_connection
def _insert(table,**kw):
    cols,args =zip(*kw.iteritems())
    sql = 'insert into `%s`(%s) value %s' % (table,',' .join(['`%s`'% col for col in cols]),','.join(['?' for i in range(len(cols))]))
    return _updata(sql,*args)


class DBError(Exception):
    pass

class MultiColumnsError(DBError):
    pass

