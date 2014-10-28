/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProvider;
import org.neo4j.kernel.impl.api.index.inmemory.InMemoryIndexProviderFactory;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.counts.CountsTracker;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.index_background_sampling_enabled;

public class RebuildCountsTest
{
    // Indexing counts are recovered/rebuild in IndexingService.start()

    @Test
    public void shouldRebuildMissingCountsStoreOnStart()
    {
        // given
        createAliensAndHumans();

        // when
        deleteCountsAndRestart();

        // then
        CountsTracker tracker = neoStore().getCounts();
        assertEquals( 32, tracker.nodeCount( -1 ) );
        assertEquals( 16, tracker.nodeCount( labelId( ALIEN ) ) );
        assertEquals( 16, tracker.nodeCount( labelId( HUMAN ) ) );
    }

    private void createAliensAndHumans()
    {
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 32; i++ )
            {
                db.createNode( i % 2 == 0 ? ALIEN : HUMAN );
            }
            tx.success();
        }
    }

    private int labelId( Label alien )
    {
        try ( Transaction tx = db.beginTx() )
        {
            return statement().readOperations().labelGetForName( alien.name() );
        }
    }

    private Statement statement()
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver()
                                      .resolveDependency( ThreadToStatementContextBridge.class )
                                      .instance();
    }

    private NeoStore neoStore()
    {
        return ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency( NeoStore.class );
    }

    public static final Label ALIEN = label( "Alien" );
    public static final Label HUMAN = label( "Human" );

    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private final InMemoryIndexProvider indexProvider = new InMemoryIndexProvider( 100 );

    private TestGraphDatabaseFactory dbFactory;
    private GraphDatabaseService db;
    private final File storeDir = new File( "/store/" );

    @Before
    public void before()
    {
        setupDb( fsRule.get() );
    }

    private void setupDb( EphemeralFileSystemAbstraction fs )
    {
        dbFactory = new TestGraphDatabaseFactory();
        dbFactory.setFileSystem( fs );
        dbFactory.addKernelExtension( new InMemoryIndexProviderFactory( indexProvider ) );
        fs.mkdirs( storeDir );
        GraphDatabaseBuilder builder = dbFactory.newImpermanentDatabaseBuilder( storeDir.getAbsolutePath() );
        builder.setConfig( index_background_sampling_enabled, "false" );
        db = builder.newGraphDatabase();
    }

    public void deleteCountsAndRestart()
    {
        db.shutdown();
        EphemeralFileSystemAbstraction snapshot = fsRule.get().snapshot();

        final File storeFileBase = new File( storeDir, NeoStore.DEFAULT_NAME + StoreFactory.COUNTS_STORE );
        File alpha = new File( storeFileBase + CountsTracker.ALPHA );
        File beta = new File( storeFileBase + CountsTracker.BETA );
        if ( snapshot.fileExists( alpha ) )
        {
            snapshot.deleteFile( alpha );
        }
        if ( snapshot.fileExists( beta ) )
        {
            snapshot.deleteFile( beta );
        }

        setupDb( snapshot );
    }

    @After
    public void after()
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
