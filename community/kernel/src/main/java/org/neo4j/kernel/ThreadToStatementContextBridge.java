package org.neo4j.kernel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;

/**
 * This is meant to serve as the bridge that makes the Beans API tie transactions to threads. The Beans API
 * will use this to get the appropriate {@link StatementContext} when it performs operations.
 */
public class ThreadToStatementContextBridge
{

    private final StatementContext readOnlyStatementCtx;
    private final ThreadLocal<TransactionContext> transactionContextForThread = new ThreadLocal<TransactionContext>();
    private final Map<TransactionContext, StatementContext> currentStatementCtx =
            new ConcurrentHashMap<TransactionContext, StatementContext>();

    public ThreadToStatementContextBridge(StatementContext readOnlyStatementCtx)
    {
        this.readOnlyStatementCtx = readOnlyStatementCtx;
    }

    public StatementContext getCtxForReading()
    {
        TransactionContext txCtx = transactionContextForThread.get();
        if(txCtx != null)
        {
            return contextForTransaction(txCtx);
        }

        return readOnlyStatementCtx;
    }
    public StatementContext getCtxForWriting()
    {
        TransactionContext txCtx = transactionContextForThread.get();
        if(txCtx != null)
        {
            return contextForTransaction(txCtx);
        }

        throw new NotInTransactionException( "You have to start a transaction to perform write operations." );
    }

    private StatementContext contextForTransaction( TransactionContext txCtx )
    {
        StatementContext stmtCtx = currentStatementCtx.get( txCtx );
        if(stmtCtx == null)
        {
            stmtCtx = txCtx.newStatementContext();
            currentStatementCtx.put( txCtx, stmtCtx );
        }

        return stmtCtx;
    }

    public void setTransactionContextForThread( TransactionContext ctx )
    {
        transactionContextForThread.set( ctx );
    }

    public void clearThisThread()
    {
        currentStatementCtx.remove( transactionContextForThread.get() );
        transactionContextForThread.remove();
    }
}
