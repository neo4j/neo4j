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
package counts;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.index_background_sampling_enabled;
import static org.neo4j.internal.kernel.api.Transaction.Type.explicit;
import static org.neo4j.internal.kernel.api.security.LoginContext.AUTH_DISABLED;
import static org.neo4j.logging.AssertableLogProvider.LogMatcherBuilder;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class RebuildCountsTest
{
    private static final int ALIENS = 16;
    private static final int HUMANS = 16;
    private static final Label ALIEN = label( "Alien" );
    private static final Label HUMAN = label( "Human" );

    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private final AssertableLogProvider internalLogProvider = new AssertableLogProvider();

    private GraphDatabaseService db;
    private File storeDir;

    @Before
    public void before() throws IOException
    {
        storeDir = testDirectory.databaseDir();
        restart( fsRule.get() );
    }

    @After
    public void after()
    {
        doCleanShutdown();
    }

    @Test
    public void shouldRebuildMissingCountsStoreOnStart() throws IOException, TransactionFailureException
    {
        // given
        createAliensAndHumans();

        // when
        FileSystemAbstraction fs = shutdown();
        deleteCounts( fs );
        restart( fs );

        // then
        try ( org.neo4j.internal.kernel.api.Transaction tx = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( Kernel.class )
                .beginTransaction( explicit, AUTH_DISABLED ) )
        {
            assertEquals( ALIENS + HUMANS, tx.dataRead().countsForNode( -1 ) );
            assertEquals( ALIENS, tx.dataRead().countsForNode( labelId( ALIEN ) ) );
            assertEquals( HUMANS, tx.dataRead().countsForNode( labelId( HUMAN ) ) );
        }

        // and also
        LogMatcherBuilder matcherBuilder = inLog( MetaDataStore.class );
        internalLogProvider.assertAtLeastOnce( matcherBuilder.warn( "Missing counts store, rebuilding it." ) );
        internalLogProvider.assertAtLeastOnce( matcherBuilder.warn( "Counts store rebuild completed." ) );
    }

    @Test
    public void shouldRebuildMissingCountsStoreAfterRecovery() throws IOException, TransactionFailureException
    {
        // given
        createAliensAndHumans();

        // when
        rotateLog();
        deleteHumans();
        FileSystemAbstraction fs = crash();
        deleteCounts( fs );
        restart( fs );

        // then
        try ( org.neo4j.internal.kernel.api.Transaction tx = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency( Kernel.class )
                .beginTransaction( explicit, AUTH_DISABLED ) )
        {
            assertEquals( ALIENS, tx.dataRead().countsForNode( -1 ) );
            assertEquals( ALIENS, tx.dataRead().countsForNode( labelId( ALIEN ) ) );
            assertEquals( 0, tx.dataRead().countsForNode( labelId( HUMAN ) ) );
        }

        // and also
        LogMatcherBuilder matcherBuilder = inLog( MetaDataStore.class );
        internalLogProvider.assertAtLeastOnce( matcherBuilder.warn( "Missing counts store, rebuilding it." ) );
        internalLogProvider.assertAtLeastOnce( matcherBuilder.warn( "Counts store rebuild completed." ) );
    }

    private void createAliensAndHumans()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < ALIENS; i++ )
            {
                db.createNode( ALIEN );
            }
            for ( int i = 0; i < HUMANS; i++ )
            {
                db.createNode( HUMAN );
            }
            tx.success();
        }
    }

    private void deleteHumans()
    {
        try ( Transaction tx = db.beginTx() )
        {
            try ( ResourceIterator<Node> humans = db.findNodes( HUMAN ) )
            {
                while ( humans.hasNext() )
                {
                    humans.next().delete();
                }
            }
            tx.success();
        }
    }

    private int labelId( Label alien )
    {
        ThreadToStatementContextBridge contextBridge = ((GraphDatabaseAPI) db).getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class );
        try ( Transaction tx = db.beginTx() )
        {
            return contextBridge.getKernelTransactionBoundToThisThread( true )
                    .tokenRead().nodeLabel( alien.name() );
        }
    }

    private void deleteCounts( FileSystemAbstraction snapshot )
    {
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        File alpha = databaseLayout.countStoreA();
        File beta = databaseLayout.countStoreB();
        assertTrue( snapshot.deleteFile( alpha ) );
        assertTrue( snapshot.deleteFile( beta ) );
    }

    private FileSystemAbstraction shutdown()
    {
        doCleanShutdown();
        return fsRule.get().snapshot();
    }

    private void rotateLog() throws IOException
    {
        ((GraphDatabaseAPI) db).getDependencyResolver()
                               .resolveDependency( CheckPointer.class ).forceCheckPoint( new SimpleTriggerInfo( "test" ) );
    }

    private FileSystemAbstraction crash()
    {
        return fsRule.get().snapshot();
    }

    private void restart( FileSystemAbstraction fs ) throws IOException
    {
        if ( db != null )
        {
            db.shutdown();
        }

        fs.mkdirs( storeDir );
        TestGraphDatabaseFactory dbFactory = new TestGraphDatabaseFactory();
        db = dbFactory.setUserLogProvider( userLogProvider )
                      .setInternalLogProvider( internalLogProvider )
                      .setFileSystem( new UncloseableDelegatingFileSystemAbstraction( fs ) )
                      .newImpermanentDatabaseBuilder( storeDir )
                      .setConfig( index_background_sampling_enabled, "false" )
                      .newGraphDatabase();
    }

    private void doCleanShutdown()
    {
        try
        {
            db.shutdown();
        }
        finally
        {
            db = null;
        }
    }
}
