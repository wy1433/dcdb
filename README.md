## What is Engine of DCDB?
DCDB is A Distributed Column-based Database. 
Engine is a interface demo for DCDB. It a parser for user's SQL like API, and manages MetaNode and DataNodes.
It is  using Flask as a web server framework and support http post api to client.


## Features
* SQL like API

    + DataType
      - int32
      - int64
      - string
      - double : `NO`
      - float	 : `NO`
      - uint32 
      - uint64	
      - bool   : `NO`
    + DML
      + SELECT - 从数据库表中获取数据
          - SELECT 列名称
          - SELECT *	: `NO`
      + UPDATE - 更新数据库表中的数据
      + DELETE - 从数据库表中删除数据
      + INSERT INTO - 向数据库表中插入数据
      
    + DML子句
      + SELECT DISTINCT : `NO`
      + SQL WHERE 子句 - WHERE 列 运算符 值
          - =
          - <> : `NO`
          - >  : `NO`
          - <  : `NO`
          - >=	 
          - <=
          - BETWEEN:`NO`
          - IN:`NO`
          - LIKE : `NO`
          - NOT : `NO`
      + SQL AND & OR 运算符
      + SQL ORDER BY 子句 :`NO`
      + SQL TOP 子句 :`NO`
      + SQL 通配符 :`NO`
      + SQL JOIN :`NO`
      + SQL UNION :`NO`
      + SQL 函数 :`NO`
      + SQL INDEX
           - 唯一索引：`NO`
           - 非唯一索引
           - 全文索引：`NO`
      + SQL 事物 : `NO`   
    + DDL `NO`
      - CREATE DATABASE - 创建新数据库
      - ALTER DATABASE - 修改数据库
      - CREATE TABLE - 创建新表
      - ALTER TABLE - 变更（改变）数据库表
      - DROP TABLE - 删除表
      - CREATE INDEX - 创建索引（搜索键）
      - DROP INDEX - 删除索引
      

