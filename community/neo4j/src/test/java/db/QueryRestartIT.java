/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package db;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.function.LongSupplier;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactoryState;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContext;
import org.neo4j.io.pagecache.tracing.cursor.context.VersionContextSupplier;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.context.TransactionVersionContext;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;

public class QueryRestartIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private GraphDatabaseService database;
    private TestTransactionVersionContextSupplier testContextSupplier;
    private File storeDir;
    private TestVersionContext testCursorContext;

    @Before
    public void setUp()
    {
        storeDir = testDirectory.directory();
        testContextSupplier = new TestTransactionVersionContextSupplier();
        database = startSnapshotQueryDb();
        createData();

        testCursorContext = testCursorContext();
        testContextSupplier.setCursorContext( testCursorContext );
    }

    @After
    public void tearDown()
    {
        if ( database != null )
        {
            database.shutdown();
        }
    }

    @Test
    public void executeQueryWithoutRestarts()
    {
        testCursorContext.setWrongLastClosedTxId( false );

        Result result = database.execute( "MATCH (n:label) RETURN n.c" );
        while ( result.hasNext() )
        {
            assertEquals( "d", result.next().get( "n.c" ) );
        }
        assertEquals( 0, testCursorContext.getAdditionalAttempts() );
    }

    @Test
    public void executeQueryWithSingleRetry()
    {
        Result result = database.execute( "MATCH (n) RETURN n.c" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        while ( result.hasNext() )
        {
            assertEquals( "d", result.next().get( "n.c" ) );
        }
    }

    @Test
    public void executeCountStoreQueryWithSingleRetry()
    {
        Result result = database.execute( "MATCH (n:toRetry) RETURN count(n)" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        while ( result.hasNext() )
        {
            assertEquals( 1L, result.next().get( "count(n)" ) );
        }
    }

    @Test
    public void executeLabelScanQueryWithSingleRetry()
    {
        Result result = database.execute( "MATCH (n:toRetry) RETURN n.c" );
        assertEquals( 1, testCursorContext.getAdditionalAttempts() );
        while ( result.hasNext() )
        {
            assertEquals( "d", result.next().get( "n.c" ) );
        }
    }

    @Test
    public void queryThatModifyDataAndSeeUnstableSnapshotThrowException()
    {
        try
        {
            database.execute( "MATCH (n:toRetry) CREATE () RETURN n.c" );
        }
        catch ( QueryExecutionException e )
        {
            assertEquals( "Unable to get clean data snapshot for query " +
                    "'MATCH (n:toRetry) CREATE () RETURN n.c' that perform updates.", e.getMessage() );
        }
    }

    private GraphDatabaseService startSnapshotQueryDb()
    {
        return new CustomGraphDatabaseFactory( new CustomFacadeFactory() )
                .newEmbeddedDatabaseBuilder( storeDir )
                .setConfig( GraphDatabaseSettings.snapshot_query, Settings.TRUE )
                .newGraphDatabase();
    }

    private void createData()
    {
        Label label = Label.label( "toRetry" );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = database.createNode( label );
            node.setProperty( "c", "d" );
            transaction.success();
        }
    }

    private TestVersionContext testCursorContext()
    {
        TransactionIdStore transactionIdStore = getTransactionIdStore();
        return new TestVersionContext( transactionIdStore::getLastClosedTransactionId );
    }

    private TransactionIdStore getTransactionIdStore()
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) database).getDependencyResolver();
        return dependencyResolver.resolveDependency( TransactionIdStore.class );
    }

    private class CustomGraphDatabaseFactory extends TestGraphDatabaseFactory
    {
        private GraphDatabaseFacadeFactory customFacadeFactory;

        CustomGraphDatabaseFactory( GraphDatabaseFacadeFactory customFacadeFactory )
        {
            this.customFacadeFactory = customFacadeFactory;
        }

        @Override
        protected GraphDatabaseBuilder.DatabaseCreator createDatabaseCreator( File storeDir,
                GraphDatabaseFactoryState state )
        {
            return new GraphDatabaseBuilder.DatabaseCreator()
            {
                @Override
                public GraphDatabaseService newDatabase( Map<String,String> config )
                {
                    return newDatabase( Config.defaults( config ) );
                }

                @Override
                public GraphDatabaseService newDatabase( Config config )
                {
                    return customFacadeFactory.newFacade( storeDir, config,
                            GraphDatabaseDependencies.newDependencies( state.databaseDependencies() ) );
                }
            };
        }
    }

    private class CustomFacadeFactory extends GraphDatabaseFacadeFactory
    {

        CustomFacadeFactory()
        {
            super( DatabaseInfo.COMMUNITY, CommunityEditionModule::new );
        }

        @Override
        protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies,
                GraphDatabaseFacade graphDatabaseFacade )
        {
            return new PlatformModule( storeDir, config, databaseInfo, dependencies, graphDatabaseFacade )
            {
                @Override
                protected VersionContextSupplier createCursorContextSupplier( Config config )
                {
                    return testContextSupplier != null ? testContextSupplier : super.createCursorContextSupplier(config);
                }
            };
        }
    }

    private class TestVersionContext extends TransactionVersionContext
    {

        private boolean wrongLastClosedTxId = true;
        private int additionalAttempts;

        TestVersionContext( LongSupplier transactionIdSupplier )
        {
            super( transactionIdSupplier );
        }

        @Override
        public long lastClosedTransactionId()
        {
            return wrongLastClosedTxId ? TransactionIdStore.BASE_TX_ID : super.lastClosedTransactionId();
        }

        @Override
        public void markAsDirty()
        {
            super.markAsDirty();
            wrongLastClosedTxId = false;
        }

        void setWrongLastClosedTxId( boolean wrongLastClosedTxId )
        {
            this.wrongLastClosedTxId = wrongLastClosedTxId;
        }

        @Override
        public boolean isDirty()
        {
            boolean dirty = super.isDirty();
            if ( dirty )
            {
                additionalAttempts++;
            }
            return dirty;
        }

        int getAdditionalAttempts()
        {
            return additionalAttempts;
        }
    }

    private class TestTransactionVersionContextSupplier extends TransactionVersionContextSupplier
    {
        void setCursorContext( VersionContext versionContext )
        {
            this.cursorContext.set( versionContext );
        }
    }
}
