package org.neo4j.impl.transaction;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

public class SpringTransactionManager implements TransactionManager
{
    static TransactionManager tm;

    public void begin() throws NotSupportedException, SystemException
    {
        tm.begin();
    }

    public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException
    {
        tm.commit();
    }

    public int getStatus() throws SystemException
    {
        return tm.getStatus();
    }

    public Transaction getTransaction() throws SystemException
    {
        return tm.getTransaction();
    }

    public void resume( Transaction tx ) throws InvalidTransactionException, IllegalStateException, SystemException
    {
        tm.resume( tx );
    }

    public void rollback() throws IllegalStateException, SecurityException, SystemException
    {
        tm.rollback();
    }

    public void setRollbackOnly() throws IllegalStateException, SystemException
    {
        tm.setRollbackOnly();
    }

    public void setTransactionTimeout( int sec ) throws SystemException
    {
        tm.setTransactionTimeout( sec );
    }

    public Transaction suspend() throws SystemException
    {
        return tm.suspend();
    }
}
