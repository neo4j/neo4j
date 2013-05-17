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
package org.neo4j.kernel.impl.event;

import java.util.concurrent.atomic.AtomicReference;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.logging.BufferingLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.test.ImpermanentGraphDatabase;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestKernelPanic
{
    @Test
    public void panicTest() throws Exception
    {
        final AtomicReference<BufferingLogger> logger = new AtomicReference<BufferingLogger>();
        @SuppressWarnings("deprecation")
        GraphDatabaseService graphDb = new ImpermanentGraphDatabase()
        {
            @Override
            protected Logging createLogging()
            {
                logger.set( new BufferingLogger() );
                return new SingleLoggingService( logger.get() );
            }
        };
        XaDataSourceManager xaDs =
            ((GraphDatabaseAPI)graphDb).getXaDataSourceManager();
        
        IllBehavingXaDataSource noob = new IllBehavingXaDataSource(UTF8.encode( "554342" ), "noob");
        xaDs.registerDataSource( noob );
        
        Panic panic = new Panic();
        graphDb.registerKernelEventHandler( panic );
     
        org.neo4j.graphdb.Transaction gdbTx = graphDb.beginTx();
        TransactionManager txMgr = ((GraphDatabaseAPI)graphDb).getTxManager();
        Transaction tx = txMgr.getTransaction();
        
        graphDb.createNode();
        noob.getXaConnection().enlistResource( tx );
        try
        {
            gdbTx.success();
            gdbTx.finish();
            fail( "Should fail" );
        }
        catch ( Throwable t )
        {
            // ok
            for ( int i = 0; i < 10 && !panic.panic; i++ )
            {
                Thread.sleep( 1000 );
            }
        }
        finally
        {
            graphDb.unregisterKernelEventHandler( panic );
        }
        assertTrue( panic.panic );
        assertTrue( "Log didn't contain expected string",
                logger.get().toString().contains( "at org.neo4j.kernel.impl.event.TestKernelPanic.panicTest" ) );
        graphDb.shutdown();
    }

    private static class Panic implements KernelEventHandler
    {
        boolean panic = false;
        
        @Override
        public void beforeShutdown()
        {
        }

        @Override
        public Object getResource()
        {
            return null;
        }

        @Override
        public void kernelPanic( ErrorState error )
        {
            panic = true;
        }

        @Override
        public ExecutionOrder orderComparedTo( KernelEventHandler other )
        {
            return null;
        }
    }
}
