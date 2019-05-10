# clientConn represents a connection between server and client, it maintains connection specific state,
# handles client query.
class ClientConn():
    def __init__(self, conn_id):
        self.conn_id = conn_id
    
    def Run(self, ctx):
        '''
        @param ctx: sql.server.context.Context
        '''
        ctx.session.Execute(ctx)
                
if __name__ == '__main__':
    pass