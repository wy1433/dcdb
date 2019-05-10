# -*-coding:utf-8 -*-
from model import ColumnInfo, TableInfo, FieldType, IndexType

#row_id = ColumnInfo(0, 'row_id', FieldType.INT, IndexType.UNIQUE)

Student = TableInfo(1, 'student', [
    ColumnInfo(0, 'row_id',   FieldType.INT,  IndexType.UNIQUE),
    ColumnInfo(1, 'id',       FieldType.INT,  IndexType.UNIQUE),
    ColumnInfo(2, 'name',     FieldType.STR,  IndexType.NORMAL),
    ColumnInfo(3, 'age',      FieldType.INT,  IndexType.NORMAL),
    ])

Test = TableInfo(2, 'test', [
    ColumnInfo(0, 'row_id',   FieldType.INT,  IndexType.UNIQUE),
    ColumnInfo(1, 'id',       FieldType.INT,  IndexType.UNIQUE),
    ColumnInfo(2, 'course',   FieldType.STR,  IndexType.NORMAL),
    ColumnInfo(3, 'score',    FieldType.INT,  IndexType.NORMAL),
    ColumnInfo(4, 'comment',  FieldType.STR,  IndexType.NORMAL),
    ])

TABLES = [
    Student,
    Test,
    ]
