/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.core;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.Node;
import org.neo4j.impl.AbstractNeoTestCase;

public class TestTxSuspendResume extends AbstractNeoTestCase
{
    public TestTxSuspendResume( String testName )
    {
        super( testName );
    }

    public void testMultipleTxSameThread() throws Exception
    {
        EmbeddedNeo neo2 = new EmbeddedNeo( "var/test-neo2" );
        TransactionManager tm = neo2.getConfig().getTxModule().getTxManager();
        tm.begin();
        Node refNode = neo2.getReferenceNode();
        Transaction tx1 = tm.suspend();
        tm.begin();
        refNode.setProperty( "test2", "test" );
        Transaction tx2 = tm.suspend();
        tm.resume( tx1 );
        CommitThread thread = new CommitThread( tm, tx2 );
        thread.start();
        // would wait for ever since tx2 has write lock but now we have other
        // thread thread that will commit tx2
        refNode.removeProperty( "test2" );
        assertTrue( thread.success() );
        tm.commit();
        neo2.shutdown();
    }

    private static class CommitThread extends Thread
    {
        private final TransactionManager tm;
        private final Transaction tx;
        private boolean success = false;

        CommitThread( TransactionManager tm, Transaction tx )
        {
            this.tm = tm;
            this.tx = tx;
        }

        @Override
        public synchronized void run()
        {
            try
            {
                sleep( 1000 );
                tm.resume( tx );
                tm.commit();
                success = true;
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
            }
        }

        synchronized boolean success()
        {
            return success;
        }
    }
}