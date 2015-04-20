/*
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
package counts;

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.kernel.impl.transaction.log.LogRotation;
import org.neo4j.kernel.impl.util.TestLogger;
import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.index_background_sampling_enabled;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.warn;
import static org.neo4j.register.Registers.newDoubleLongRegister;

public class RebuildCountsTest
{
    // Indexing counts are recovered/rebuild in IndexingService.start() and are not tested here

    @Test
    public void shouldRebuildMissingCountsStoreOnStart() throws IOException
    {
        // given
        createAliensAndHumans();

        // when
        FileSystemAbstraction fs = shutdown();
        deleteCounts( fs );
        restart( fs );

        // then
        CountsTracker tracker = counts();
        assertEquals( ALIENS + HUMANS, tracker.nodeCount( -1, newDoubleLongRegister() ).readSecond() );
        assertEquals( ALIENS, tracker.nodeCount( labelId( ALIEN ), newDoubleLongRegister() ).readSecond() );
        assertEquals( HUMANS, tracker.nodeCount( labelId( HUMAN ), newDoubleLongRegister() ).readSecond() );

        // and also
        logger().assertAtLeastOnce( warn( "Missing counts store, rebuilding it." ) );
    }

    @Test
    public void shouldRebuildMissingCountsStoreAfterRecovery() throws IOException
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
        CountsTracker tracker = counts();
        assertEquals( ALIENS, tracker.nodeCount( -1, newDoubleLongRegister() ).readSecond() );
        assertEquals( ALIENS, tracker.nodeCount( labelId( ALIEN ), newDoubleLongRegister() ).readSecond() );
        assertEquals( 0, tracker.nodeCount( labelId( HUMAN ), newDoubleLongRegister() ).readSecond() );

        // and also
        logger().assertAtLeastOnce( warn( "Missing counts store, rebuilding it." ) );
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
        try( Transaction tx = db.beginTx() )
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
        try ( Transaction tx = db.beginTx() )
        {
            return ((GraphDatabaseAPI) db).getDependencyResolver()
                                          .resolveDependency( ThreadToStatementContextBridge.class )
                                          .instance().readOperations().labelGetForName( alien.name() );
        }
    }

    private CountsTracker counts()
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( NeoStore.class ).getCounts();
    }

    private void deleteCounts( FileSystemAbstraction snapshot )
    {
        final File storeFileBase = new File( storeDir, NeoStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE );
        File alpha = new File( storeFileBase + CountsTracker.LEFT );
        File beta = new File( storeFileBase + CountsTracker.RIGHT );
        assertTrue( snapshot.deleteFile( alpha ) );
        assertTrue( snapshot.deleteFile( beta ) );

    }

    private FileSystemAbstraction shutdown()
    {
        doCleanShutdown();
        return fsRule.get().snapshot();
    }

    @SuppressWarnings( "deprecated" )
    private void rotateLog() throws IOException
    {
        ((GraphDatabaseAPI) db).getDependencyResolver()
                               .resolveDependency( LogRotation.class ).rotateLogFile();
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
        db = dbFactory.setLogging( logging )
                      .setFileSystem( fs )
                      .addKernelExtension( new InMemoryIndexProviderFactory( indexProvider ) )
                      .newImpermanentDatabaseBuilder( storeDir.getAbsolutePath() )
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

    private TestLogger logger()
    {
        return logging.getMessagesLog( StoreFactory.class );
    }

    private static final int ALIENS = 16;
    private static final int HUMANS = 16;
    private static final Label ALIEN = label( "Alien" );
    private static final Label HUMAN = label( "Human" );

    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final InMemoryIndexProvider indexProvider = new InMemoryIndexProvider( 100 );
    private final TestLogging logging = new TestLogging();

    private GraphDatabaseService db;
    private final File storeDir = new File( "store" ).getAbsoluteFile();

    @Before
    public void before() throws IOException
    {
        restart( fsRule.get() );
    }

    @After
    public void after()
    {
        doCleanShutdown();
    }
}
