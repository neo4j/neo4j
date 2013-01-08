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

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.UTF8;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.*;

public class TestKernelPanic
{
    @Test
    public void panicTest() throws Exception
    {
        String path = "target/var/testdb";
        AbstractNeo4jTestCase.deleteFileOrDirectory( new File( path ) );
        GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( path );
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
        assertMessageLogContains(path,"at org.neo4j.kernel.impl.event.TestKernelPanic.panicTest");
        graphDb.shutdown();
    }

    private void assertMessageLogContains(String path, String exceptionString) throws FileNotFoundException {
        final File logFile = new File(path, StringLogger.DEFAULT_NAME);
        assertTrue("exists "+logFile,logFile.exists() && logFile.isFile());
        final Scanner scanner = new Scanner(logFile).useDelimiter("\n");
        for (String line : IteratorUtil.asIterable(scanner)) {
            if (line.contains(exceptionString)) return;
        }
        fail(logFile+" did not contain: "+exceptionString);
    }

    private static class Panic implements KernelEventHandler
    {
        boolean panic = false;
        
        public void beforeShutdown()
        {
            // TODO Auto-generated method stub
            
        }

        public Object getResource()
        {
            // TODO Auto-generated method stub
            return null;
        }

        public void kernelPanic( ErrorState error )
        {
            panic = true;
        }

        public ExecutionOrder orderComparedTo( KernelEventHandler other )
        {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
}
