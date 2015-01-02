/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.util.Collection;
import java.util.concurrent.Callable;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.ErrorState;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LabelTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.nioneo.xa.TransactionWriter;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.InMemoryLogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Commit;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry.Start;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.logging.BufferingLogger;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestKernelPanic
{
    @Test( timeout = 10000 )
    public void panicTest() throws Exception
    {
        BufferingLogger logger = new BufferingLogger();
        GraphDatabaseService graphDb = new TestGraphDatabaseFactory().setLogging(
                new SingleLoggingService( logger ) ).newImpermanentDatabase();
        XaDataSourceManager xaDs =
            ((GraphDatabaseAPI)graphDb).getDependencyResolver().resolveDependency( XaDataSourceManager.class );
        
        IllBehavingXaDataSource adversarialDataSource =
                new IllBehavingXaDataSource(UTF8.encode( "554342" ), "adversarialDataSource");
        xaDs.registerDataSource( adversarialDataSource );
        
        Panic panic = new Panic();
        graphDb.registerKernelEventHandler( panic );

        org.neo4j.graphdb.Transaction gdbTx = graphDb.beginTx();
        TransactionManager txMgr = ((GraphDatabaseAPI)graphDb).getDependencyResolver()
                .resolveDependency( TransactionManager.class );
        Transaction tx = txMgr.getTransaction();

        graphDb.createNode();
        adversarialDataSource.getXaConnection().enlistResource( tx );
        try
        {
            gdbTx.success();
            gdbTx.finish();
            fail( "Should fail" );
        }
        catch ( Exception t )
        {
            // It's okay, we expected this.
            // Now just wait until we observe the kernel panicking:
            //noinspection StatementWithEmptyBody
            while ( !panic.panic );
        }
        finally
        {
            graphDb.unregisterKernelEventHandler( panic );
        }

        try
        {
            assertTrue( panic.panic );
            assertThat("Log didn't contain expected string",
                    logger.toString(), containsString("at org.neo4j.kernel.impl.event.TestKernelPanic.panicTest"));
        }
        finally
        {
            graphDb.shutdown();
        }
    }

    @Test
    public void shouldPanicOnApplyTransactionFailure() throws Exception
    {
        // GIVEN
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setTransactionInterceptorProviders( asList( interceptorProviderThatBreaksStuff() ) );
        GraphDatabaseAPI db = (GraphDatabaseAPI) factory.newImpermanentDatabaseBuilder()
                .setConfig( GraphDatabaseSettings.intercept_deserialized_transactions.name(), TRUE.toString() )
                .setConfig( TransactionInterceptorProvider.class.getSimpleName() + ".breaker", TRUE.toString() )
                .newGraphDatabase();
        XaDataSourceManager dsManager = db.getDependencyResolver().resolveDependency( XaDataSourceManager.class );
        XaDataSource ds = dsManager.getXaDataSource( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME );

        // WHEN
        try
        {
            ds.applyCommittedTransaction( 2, simpleTransaction() );
            fail( "Should have failed" );
        }
        catch ( BreakageException e )
        {   // Good
        }

        // THEN
        assertNotOk( beginTransaction( db ) );
        assertNotOk( applyTransaction( ds ) );
    }

    private void assertNotOk( Callable<Void> callable )
    {
        try
        {
            callable.call();
            fail( "Should have failed saying that tm not OK");
        }
        catch ( Exception e )
        {   // Good
            assertTrue( someExceptionContainsMessage( e, "Kernel has encountered some problem" ) );
        }
    }

    private boolean someExceptionContainsMessage( Throwable e, String string )
    {
        while ( e != null )
        {
            if ( e.getMessage().contains( string ) )
            {
                return true;
            }
            e = e.getCause();
        }
        return false;
    }

    private Callable<Void> applyTransaction( final XaDataSource ds )
    {
        return new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                ds.applyCommittedTransaction( 2, simpleTransaction() );
                return null;
            }
        };
    }

    private Callable<Void> beginTransaction( final GraphDatabaseService db )
    {
        return new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                db.beginTx();
                return null;
            }
        };
    }

    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();


    private ReadableByteChannel simpleTransaction() throws IOException
    {
        InMemoryLogBuffer buffer = new InMemoryLogBuffer();
        TransactionWriter writer = new TransactionWriter( buffer, 1, -1 );

        writer.start( -1, -1, 0 );
        writer.add( new NodeRecord( 0, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue() ),
                new NodeRecord( 0, Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue() ) );
        writer.commit( false, 2 );
        writer.done();

        return buffer;
    }

    private TransactionInterceptorProvider interceptorProviderThatBreaksStuff()
    {
        return new TransactionInterceptorProvider( "breaker" )
        {
            @Override
            public TransactionInterceptor create( TransactionInterceptor next, XaDataSource ds, String options,
                    DependencyResolver dependencyResolver )
            {
                throw new AssertionError( "I don't think this is needed" );
            }

            @Override
            public TransactionInterceptor create( XaDataSource ds, String options, DependencyResolver dependencyResolver )
            {
                return interceptorThatBreaksStuff();
            }
        };
    }


    private TransactionInterceptor interceptorThatBreaksStuff()
    {
        return new TransactionInterceptor()
        {
            private void breakStuff()
            {
                throw new BreakageException();
            }

            @Override
            public void visitRelationship( RelationshipRecord record )
            {
                breakStuff();
            }

            @Override
            public void visitProperty( PropertyRecord record )
            {
                breakStuff();
            }

            @Override
            public void visitRelationshipTypeToken( RelationshipTypeTokenRecord record )
            {
                breakStuff();
            }

            @Override
            public void visitLabelToken( LabelTokenRecord record )
            {
                breakStuff();
            }

            @Override
            public void visitPropertyKeyToken( PropertyKeyTokenRecord record )
            {
                breakStuff();
            }

            @Override
            public void visitNode( NodeRecord record )
            {
                breakStuff();
            }

            @Override
            public void visitNeoStore( NeoStoreRecord record )
            {
                breakStuff();
            }

            @Override
            public void visitSchemaRule( Collection<DynamicRecord> records )
            {
                breakStuff();
            }

            @Override
            public void setStartEntry( Start entry )
            {
                breakStuff();
            }

            @Override
            public void setCommitEntry( Commit entry )
            {
                breakStuff();
            }

            @Override
            public void complete()
            {
                breakStuff();
            }
        };
    }

    private static class BreakageException extends RuntimeException
    {
        public BreakageException()
        {
            super( "Breaking" );
        }
    }

    private static class Panic implements KernelEventHandler
    {
        volatile boolean panic = false;
        
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
