#!/usr/bin/env python
# -*-coding:utf-8 -*-
from sql.server.context import Context
from sql.executor.executor import BeginExec,CommitExec, RollBackExec
from sql.executor.select import SelectExec
from sql.executor.insert import InsertExec
from sql.executor.delete import DeleteExec
from sql.executor.update import UpdateExec
from sql.parser.statement import *


class Planner():
    def __init__(self):
        pass
    
    def BuildExecutor(self, ctx):
        '''
        @param ctx: Context
        '''
        t =  type(ctx.stmt)
        executor = None
        if t == BeginStmt:
            executor = BeginExec(ctx)
        elif t == CommitStmt:
            executor = CommitExec(ctx)
        elif t == RollBackStmt:
            executor = RollBackExec(ctx)
        elif t == SelectStmt:
            executor = SelectExec(ctx)
        elif t == InsertStmt:
            executor = InsertExec(ctx)
        elif t == DeleteStmt:
            executor = DeleteExec(ctx)
        elif t == UpdateStmt(ctx):
            executor = UpdateExec(ctx)
            
        ctx.executor = executor
        logger.debug("BuildExecutor type=%s", t) 