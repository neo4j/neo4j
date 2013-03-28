/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.database;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.TxManager;

import javax.transaction.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class CoreAPIBasedTransactionRegistry implements TransactionRegistry
{

    private final GraphDatabaseAPI db;
    private final TxManager txManager;
    private final ConcurrentHashMap<Long, SessionTx> sessionState = new ConcurrentHashMap<Long, SessionTx>(50);

    // Associates one session with one transaction
    private static class SessionTx
    {
        private final ReentrantLock lock = new ReentrantLock();
        private final long sessionId;
        private boolean isInUse = false;
        private Transaction transaction;

        public SessionTx(long sessionId)
        {
            this.sessionId = sessionId;
        }

        void markIsUsingTransaction()
        {
            awaitNotInUse(1000 * 10);
            lock.lock();
            try
            {
                if(!isInUse)
                {
                    // Yay, we get to pick now
                    isInUse = true;
                    return;
                }
            } finally {
                lock.unlock();
            }

            // Fail, someone beat us to marking it in use after it was unmarked, retry
            markIsUsingTransaction();
        }

        void markNotUsingTransaction()
        {
            isInUse = false;
        }

        private void awaitNotInUse(long timeout) {
            long stopAt = System.currentTimeMillis() + timeout;
            while(isInUse)
            {
                Thread.yield();
                if(System.currentTimeMillis() > stopAt)
                {
                    throw new RuntimeException("Timed out waiting to acquire transaction for session " + sessionId);
                }
            }
        }

        public void setTransaction(Transaction transaction)
        {
            this.transaction = transaction;
        }

        public boolean hasTransaction()
        {
            return transaction != null;
        }

        public Transaction getTransaction()
        {
            return transaction;
        }
    }

    public CoreAPIBasedTransactionRegistry(GraphDatabaseAPI db)
    {
        this.db = db;
        this.txManager = db.getDependencyResolver().resolveDependency(TxManager.class);
    }

    @Override
    public void associateTransactionWithThread(long sessionId) {
        SessionTx sessionTx = getOrCreateSessionTx(sessionId);
        sessionTx.markIsUsingTransaction();

        if (sessionTx.hasTransaction()) {
            resumeTransaction(sessionTx);
        } else
        {
            db.beginTx();
        }
    }

    @Override
    public void disassociateTransactionWithThread(long sessionId) {
        SessionTx sessionTx = getOrCreateSessionTx(sessionId);

        Transaction tx = suspendTransaction();
        sessionTx.setTransaction(tx);

        sessionTx.markNotUsingTransaction();
    }

    @Override
    public void commitCurrentTransaction(long sessionId)
    {
        try
        {
            txManager.commit();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            getOrCreateSessionTx(sessionId).markNotUsingTransaction();
        }
    }

    @Override
    public void rollbackCurrentTransaction(long sessionId)
    {
        try
        {
            txManager.rollback();
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            getOrCreateSessionTx(sessionId).markNotUsingTransaction();
        }
    }

    @Override
    public void rollbackAll() {
        try
        {
            if(txManager.getTransaction() != null)
                txManager.rollback();
        } catch (SystemException e) {
            e.printStackTrace(); // But keep going
        }

        // Nuclear option, expects that we are single-threaded
        for (Map.Entry<Long, SessionTx> entry : sessionState.entrySet())
        {
            try
            {
                resumeTransaction(entry.getValue());
                rollbackCurrentTransaction(entry.getKey());
            } catch(RuntimeException e)
            {
                e.printStackTrace();
            }
        }

    }

    private Transaction suspendTransaction() {
        try
        {
            return txManager.suspend();
        }
        catch (SystemException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void resumeTransaction(SessionTx sessionTx) {
        try
        {
            txManager.resume(sessionTx.getTransaction());
        }
        catch (SystemException e)
        {
            throw new RuntimeException(e);
        }
    }

    private SessionTx getOrCreateSessionTx(long sessionId) {
        SessionTx sessionTx = sessionState.get(sessionId);
        if(sessionTx == null)
        {
            sessionState.putIfAbsent(sessionId, new SessionTx(sessionId));
            return sessionState.get(sessionId);
        }
        return sessionTx;
    }
}
