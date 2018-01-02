/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.PageCacheRule;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(Suite.class)
@SuiteClasses({IdGeneratorRebuildFailureEmulationTest.FailureBeforeRebuild.class})
public class IdGeneratorRebuildFailureEmulationTest
{
    @RunWith(JUnit4.class)
    public static final class FailureBeforeRebuild extends IdGeneratorRebuildFailureEmulationTest
    {
        @Override
        protected void emulateFailureOnRebuildOf( NeoStores neoStores )
        {
            // emulate a failure during rebuild by not issuing this call:
            // neostores.makeStoreOk();
        }
    }

    private void performTest( String neostoreFileName ) throws Exception
    {
        File idFile = new File( storeDir, neostoreFileName + ".id" );
        // emulate the need for rebuilding id generators by deleting it
        fs.deleteFile( idFile );
        NeoStores neoStores = null;
        try
        {
            neoStores = factory.openAllNeoStores();
            // emulate a failure during rebuild:
            emulateFailureOnRebuildOf( neoStores );
        }
        catch ( UnderlyingStorageException expected )
        {
            assertThat( expected.getMessage(), startsWith( "Id capacity exceeded" ) );
        }
        finally
        {
            // we want close to not misbehave
            // (and for example truncate the file based on the wrong highId)
            if ( neoStores != null )
            {
                neoStores.close();
            }
        }
    }

    void emulateFailureOnRebuildOf( NeoStores neoStores )
    {
        fail( "emulateFailureOnRebuildOf(NeoStores) must be overridden" );
    }

    private FileSystem fs;
    private StoreFactory factory;
    private final File storeDir = new File( "dir" ).getAbsoluteFile();

    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();

    @Before
    public void initialize()
    {
        fs = new FileSystem();
        GraphDatabaseService graphdb = new Database( storeDir );
        createInitialData( graphdb );
        graphdb.shutdown();
        Map<String, String> params = new HashMap<>();
        params.put( GraphDatabaseSettings.rebuild_idgenerators_fast.name(), Settings.FALSE );
        Config config = new Config( params, GraphDatabaseSettings.class );
        factory = new StoreFactory( storeDir, config, new DefaultIdGeneratorFactory( fs ),
                pageCacheRule.getPageCache( fs ), fs, NullLogProvider.getInstance() );
    }

    @After
    public void verifyAndDispose() throws Exception
    {
        GraphDatabaseService graphdb = null;
        try
        {
            graphdb = new Database( storeDir );
            verifyData( graphdb );
        }
        finally
        {
            if ( graphdb != null )
            {
                graphdb.shutdown();
            }
            if ( fs != null )
            {
                fs.disposeAndAssertNoOpenFiles();
            }
            fs = null;
        }
    }

    private void verifyData( GraphDatabaseService graphdb )
    {
        try ( Transaction tx = graphdb.beginTx() )
        {
            int nodecount = 0;
            for ( Node node : GlobalGraphOperations.at( graphdb ).getAllNodes() )
            {
                int propcount = readProperties( node );
                int relcount = 0;
                for ( Relationship rel : node.getRelationships() )
                {
                    assertEquals( "all relationships should have 3 properties.", 3, readProperties( rel ) );
                    relcount++;
                }
                assertEquals( "all created nodes should have 3 properties.", 3, propcount );
                assertEquals( "all created nodes should have 2 relationships.", 2, relcount );

                nodecount++;
            }
            assertEquals( "The database should have 2 nodes.", 2, nodecount );
        }
    }

    private void createInitialData( GraphDatabaseService graphdb )
    {
        try ( Transaction tx = graphdb.beginTx() )
        {
            Node first = properties( graphdb.createNode() );
            Node other = properties( graphdb.createNode() );
            properties( first.createRelationshipTo( other, DynamicRelationshipType.withName( "KNOWS" ) ) );
            properties( other.createRelationshipTo( first, DynamicRelationshipType.withName( "DISTRUSTS" ) ) );

            tx.success();
        }
    }

    private <E extends PropertyContainer> E properties( E entity )
    {
        entity.setProperty( "short thing", "short" );
        entity.setProperty( "long thing",
                "this is quite a long string, don't you think, it sure is long enough at least" );
        entity.setProperty( "string array", new String[]{"these are a few", "cool strings",
                "for your viewing pleasure"} );
        return entity;
    }

    private int readProperties( PropertyContainer entity )
    {
        int count = 0;
        for ( String key : entity.getPropertyKeys() )
        {
            entity.getProperty( key );
            count++;
        }
        return count;
    }

    private static class FileSystem extends EphemeralFileSystemAbstraction
    {
        void disposeAndAssertNoOpenFiles() throws Exception
        {
            //Collection<String> open = openFiles();
            //assertTrue( "Open files: " + open, open.isEmpty() );
            assertNoOpenFiles();
            super.shutdown();
        }

        @Override
        public void shutdown()
        {
            // no-op, it's pretty odd to have EphemeralFileSystemAbstraction implement Lifecycle by default
        }
    }

    @SuppressWarnings("deprecation")
    private class Database extends ImpermanentGraphDatabase
    {
        public Database( File storeDir )
        {
            super( storeDir );
        }

        @Override
        protected void create( File storeDir, Map<String, String> params, GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            new CommunityFacadeFactory()
            {
                @Override
                protected PlatformModule createPlatform( File storeDir, Map<String, String> params,
                        Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade,
                        OperationalMode operationalMode )
                {
                    return new ImpermanentPlatformModule( storeDir, params, dependencies, graphDatabaseFacade )
                    {
                        @Override
                        protected FileSystemAbstraction createFileSystemAbstraction()
                        {
                            return fs;
                        }
                    };
                }
            }.newFacade( storeDir, params, dependencies, this );
        }

    }

    @Test
    public void neostore() throws Exception
    {
        performTest( MetaDataStore.DEFAULT_NAME );
    }

    @Test
    public void neostore_nodestore_db() throws Exception
    {
        performTest( MetaDataStore.DEFAULT_NAME + StoreFactory.NODE_STORE_NAME );
    }

    @Test
    public void neostore_propertystore_db_arrays() throws Exception
    {
        performTest( MetaDataStore.DEFAULT_NAME + StoreFactory.PROPERTY_ARRAYS_STORE_NAME );
    }

    @Test
    public void neostore_propertystore_db() throws Exception
    {
        performTest( MetaDataStore.DEFAULT_NAME + StoreFactory.PROPERTY_STORE_NAME );
    }

    @Test
    public void neostore_propertystore_db_index() throws Exception
    {
        performTest( MetaDataStore.DEFAULT_NAME + StoreFactory.PROPERTY_KEY_TOKEN_STORE_NAME );
    }

    @Test
    public void neostore_propertystore_db_index_keys() throws Exception
    {
        performTest( MetaDataStore.DEFAULT_NAME + StoreFactory.PROPERTY_KEY_TOKEN_NAMES_STORE_NAME );
    }

    @Test
    public void neostore_propertystore_db_strings() throws Exception
    {
        performTest( MetaDataStore.DEFAULT_NAME + StoreFactory.PROPERTY_STRINGS_STORE_NAME );
    }

    @Test
    public void neostore_relationshipstore_db() throws Exception
    {
        performTest( MetaDataStore.DEFAULT_NAME + StoreFactory.RELATIONSHIP_STORE_NAME );
    }

    @Test
    public void neostore_relationshiptypestore_db() throws Exception
    {
        performTest( MetaDataStore.DEFAULT_NAME + StoreFactory.RELATIONSHIP_TYPE_TOKEN_STORE_NAME );
    }

    @Test
    public void neostore_relationshiptypestore_db_names() throws Exception
    {
        performTest( MetaDataStore.DEFAULT_NAME + StoreFactory.RELATIONSHIP_TYPE_TOKEN_NAMES_STORE_NAME );
    }

    private IdGeneratorRebuildFailureEmulationTest()
    {
        if ( IdGeneratorRebuildFailureEmulationTest.class == getClass() )
        {
            throw new UnsupportedOperationException( "This class is effectively abstract" );
        }
    }
}
