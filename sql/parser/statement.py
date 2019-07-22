#!/usr/bin/env python
# -*-coding:utf-8 -*-
# from sql.table.column import ColumnInfo

import re
from util.error import ErrInvalidSql
from enum import IntEnum
from mylog import logger

class ResultField(object):
    def __init__(self, ColumnInfo = None, Referenced = False):
        '''
        @type Column: ColumnInfo
        '''
        self.ColumnInfo = ColumnInfo
        # Referenced indicates the result field has been referenced or not.
        # If not, we don't need to get the values.
        self.referenced = Referenced
        self.pairs = dict() #k,v
    
    def __and__(self, rhs):
        '''
        @type rhs: ResultField
        '''
        return dict.fromkeys(self.pairs.viewkeys() & rhs.pairs.viewkeys())
        
    def __or__(self, rhs):
        return dict.fromkeys(self.pairs.viewkeys() | rhs.pairs.viewkeys())


def PreParse(sql):
    sql = sql.strip()
    p=re.compile(r'(.*?between.*?)(and)(.*)', re.I)
    sql = p.sub(r'\1 between_and\3',sql)
    return sql

    
class Statement(object):
    def __init__(self, text=None):
        '''
        @param text: str
        '''
        if text:
            self.text = text.strip().rstrip(';')
        
    def Parse(self):
        pass

class BeginStmt(Statement):
    def Parse(self):
        keyword = self.text.lower()
        if keyword =='begin':
            return None
        else:
            return ErrInvalidSql

class CommitStmt(Statement):
    def Parse(self):
        keyword = self.text.lower()
        if keyword =='commit':
            return None
        else:
            return ErrInvalidSql

class RollBackStmt(Statement):
    def Parse(self):
        keyword = self.text.lower()
        if keyword =='rollback':
            return None
        else:
            return ErrInvalidSql


class ExprNode(Statement):
    def __init__(self, text=None):
        super(ExprNode, self).__init__(text)
#         self.ResultField = ResultField()
        
    def Parse(self):
        pass
    
    @staticmethod
    def GetExpr(text):
        p = re.compile(r'(.*)\s+(and|or)\s+(.*)', re.I)
        m = p.match(text)
        if m:
            l, op, r = m.groups()
            if op.lower() == 'and':
                expr = IntersectionExpr('and', ltext=l, rtext=r)
            else:
                expr = UnionExpr('or', ltext=l, rtext=r)
        else:
            expr = ConditionExpr(text)
        expr.Parse()
        return expr
    
    def __repr__(self, level=0):
        # PreOrderTraverse
#         ret = "\t"*level+repr(self.text)+"\n"
#         if not isinstance(self, ConditionExpr):
#             ret += self.lexpr.__repr__(level+1)
#             ret += self.rexpr.__repr__(level+1)
#         return ret
        
        # InOrderTraverse
        if isinstance(self, ConditionExpr):
            ret = "\t"*level+repr(self.text)+"\n"
        else:
            ret = self.lexpr.__repr__(level+1)
            ret += "\t"*level+repr(self.text)+"\n"
            ret += self.rexpr.__repr__(level+1)
        return ret
    
        

# class ConditionType(IntEnum):
#     ConditionTypeEquals       = 1
#     ConditionTypeIn           = 2
#     ConditionTypeRangeScan    = 3
   

class ConditionExpr(ExprNode):
    ''' Expression like this:
        c > start
        c < end
        c between start and end
        c = v
        c in (v1, v2, v3)
    '''
    def __init__(self, text, column=None, start=None, end=None, 
                 value=None, values=None, include_start=False, include_end=False):
        '''
        @param column: ColumnInfo
        @param start: start value of column, None declare no limit.
        @param end: end value of column, None decalare no limit.
        @attention: if values is not None, "start", "end" will be ignore.
        '''
        super(ConditionExpr, self).__init__(text)
        self.column = column
        self.start = start
        self.end = end
        self.value = value
        self.values = values
        self.include_start = include_start
        self.include_end = include_end
        self.table = None
    
    def Parse(self):
#         infos = self.text.split()
        
        infos = re.split('[()\"\',\s]',self.text)
        infos = [x for x in infos if x]

        self.column = infos[0]
        op = infos[1].lower()
        if op == '>' :
            self.start = infos[2].strip('(),\'\"')
            self.include_start = False
        elif op == '>=':
            self.start = infos[2].strip('(),\'\"')
            self.include_start = True
        elif op == '<':
            self.end = infos[2].strip('(),\'\"')
            self.include_end = False
        elif op == '<=':
            self.end = infos[2].strip('(),\'\"')
            self.include_end = True
        elif op == 'between':
            self.start = infos[2].strip('(),\'\"')
            self.include_start = True
            self.end = infos[4].strip('(),\'\"')
            self.include_end = True
        elif op == '=':
            self.value = infos[2].strip('(),\'\"')
        elif op == 'in':
            self.values = [x.strip('(),\'\"') for x in infos[2:]]
        else:
            return ErrInvalidSql
        
        logger.debug("col=%s,start=%s,si=%s,end=%s,ei=%s,value=%s,values=%s",
                     self.column, 
                     self.start, self.include_start,
                     self.end, self.include_end,
                     self.value, self.values
                     )
            

class UnionExpr(ExprNode):
    ''' Expression like this:
        ExprNode OR ExprNode
    '''
    def __init__(self, text,  ltext=None, rtext=None, lexpr=None, rexpr=None):
        '''
        @param lexpr: ExprNode , left operand
        @param rexpr: ExprNode , right operand
        '''
        super(UnionExpr, self).__init__(text)
        self.ltext = ltext
        self.rtext = rtext
        self.lexpr = lexpr
        self.rexpr = rexpr
    
    def Parse(self):
        self.lexpr = ExprNode.GetExpr(self.ltext)
        self.rexpr = ExprNode.GetExpr(self.rtext)
 
        
class IntersectionExpr(ExprNode):
    ''' Expression like this:
        ExprNode OR ExprNode
    '''
    def __init__(self, text, ltext=None, rtext=None, lexpr=None, rexpr=None):
        '''
        @param lexpr: ExprNode , left operand
        @param rexpr: ExprNode , right operand
        '''
        super(IntersectionExpr, self).__init__(text)
        self.ltext = ltext
        self.rtext = rtext
        self.lexpr = lexpr
        self.rexpr = rexpr
        
    def Parse(self):
        self.lexpr = ExprNode.GetExpr(self.ltext)
        self.rexpr = ExprNode.GetExpr(self.rtext)

    
# SelectStmt represents the select query node.
class SelectStmt(Statement):
    def __init__(self, text):
        super(SelectStmt, self).__init__(text)
        self.ResultSet = list() # type: list[ResultField]
        self.Table = None
        # Where is the where clause in select statement.
        self.Where = ExprNode()
        # Fields is the select expression list.
        self.Fields = list() # type list[ColumnInfo]
        # GroupBy is the group by expression list.
        self.GroupBy = None # no implemented yet
        # Having is the having condition.
        self.Having = None # no implemented yet
        # OrderBy is the ordering expression list.
        self.OrderBy = None # no implemented yet
        # Limit is the limit clause.
        self.Limit = None # no implemented yet
        # LockTp is the lock type
        self.SelectLockForUpdate = False
    
    def Parse(self):
#         sql = '''SELECT a, b, c From t 
#                 WHERE a > 10 
#                 AND   b < -10 
#                 OR    d between -10 and 10
#                 OR    e in ("a","b","c")
#                 AND   f = "foo"
#                 '''
        p = re.compile(r'(.*)(for\s+update)', re.I)
        m = p.search(self.text)
        if m:
            self.SelectLockForUpdate = True
            text = m.groups[0]
        else:
            text = self.text
        
        p = re.compile(r"select\s+(.*?)\s+from\s+(.*?)\s+where\s+(.*)", re.I)
        m = p.match(text)
        if m:
            s, t, w = m.groups()
            self.Fields = s.split(',')
            self.Fields = [f.strip() for f in self.Fields]
            self.Table = t
            self.Where = ExprNode.GetExpr(w)
        else:
            return ErrInvalidSql
        
# # Assignment is the expression for assignment, like a = 1.
# class Assignment(Statement):
#     def __init__(self, text):
#         super(Assignment, self).__init__(text)
#         self.ColumnName
#         self.Value

# InsertStmt is a statement to insert new rows into an existing table.
# See https://dev.mysql.com/doc/refman/5.7/en/insert.html
class InsertStmt(Statement):
    def __init__(self, text):
        super(InsertStmt, self).__init__(text)
        self.Table = None
        # Fields is the select expression list.
        self.Fields = list() # type list[ColumnInfo]
        self.Setlist = list() # list[value of column to be set]
        
    def Parse(self):
        '''sql like this:
        INSERT INTO table_name (column1,column2,column3,...)
        VALUES (value1,value2,value3,...);
        '''        
        p = re.compile(r"insert\s+into\s+(.*?)\s+\((.*?)\)\s+values\s+\((.*?)\)", re.I)
        m = p.match(self.text)
        if m:
            t, c, v = m.groups()
            self.Table = t
            self.Fields = c.split(',')
            self.Fields = [f.strip() for f in self.Fields]
            self.Setlist = v.split(',')
            self.Setlist = [v.strip().strip("\'\"") for v in self.Setlist]
            logger.debug('InsertStmt: table=%s, fields=%s, values=%s', 
                         self.Table, self.Fields, self.Setlist)
        else:
            return ErrInvalidSql

# DeleteStmt is a statement to delete rows from table.
# See https://dev.mysql.com/doc/refman/5.7/en/delete.html
class DeleteStmt(Statement):
    def __init__(self, text):
        super(DeleteStmt, self).__init__(text)
        self.ResultSet = list() # type: list[ResultField]
        self.Table = None
        # Where is the where clause in select statement.
        self.Where = ExprNode()
    
    def Parse(self):        
        '''sql like this:
        DELETE FROM table_name
        WHERE some_column=some_value;
        '''
        p = re.compile(r"delete\s+from\s+(.*?)\s+where\s+(.*)", re.I)
        m = p.match(self.text)
        if m:
            f, w = m.groups()
            self.Table = f
            self.Where = ExprNode.GetExpr(w)
        else:
            return ErrInvalidSql

# UpdateStmt is a statement to update columns of existing rows in tables with new values.
# See https://dev.mysql.com/doc/refman/5.7/en/update.html
class UpdateStmt(Statement):
    def __init__(self, text):
        super(UpdateStmt, self).__init__(text)
        self.ResultSet = list() # type: list[ResultField]
        self.Table = None
        # Where is the where clause in select statement.
        self.Where = ExprNode()
        # Fields is the select expression list.
        self.Fields = list() # type list[ColumnInfo]
        self.Setlist = list() # list[value of column to be set]
    
    def Parse(self):        
        '''sql like this:
        UPDATE table_name
        SET column1=value1,column2=value2,...
        WHERE some_column=some_value;
        '''
        p = re.compile(r"update\s+(.*?)\s+set\s+(.*?)\s+where\s+(.*)", re.I)
        m = p.match(self.text)
        if m:
            t, s, w = m.groups()
            self.Table = t
            for ss in s.split(','):
                c, v = ss.split('=')
                c = c.strip()
                v = v.strip().strip("\'\"")
                self.Fields.append(c)
                self.Setlist.append(v)
            self.Where = ExprNode.GetExpr(w)
        else:
            return ErrInvalidSql
       
if __name__ == '__main__':
#     sql = '''SELECT a, b, c From t \
#         WHERE a > 10 \
#         AND   b < -10 \
#         OR    d between -10 in 10 \
#         OR    e in ("a","b","c") \
#         AND   f = "foo"
#         '''
#     s = SelectStmt(sql)
#     s.Parse()
    
    w = '''a > 10 \
    AND   b < -10 \
    OR    d between -10 in 10 \
    OR    e in ("a","b","c") \
    AND   f = "foo"
    '''
    s = ExprNode.GetExpr(w)
    print repr(s)
    
    