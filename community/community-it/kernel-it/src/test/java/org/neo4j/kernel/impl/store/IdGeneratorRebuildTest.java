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
package org.neo4j.kernel.impl.store;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.graphdb.mockfs.UncloseableDelegatingFileSystemAbstraction;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@EphemeralPageCacheExtension
class IdGeneratorRebuildTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private PageCache pageCache;

    private StoreFactory factory;
    private DatabaseLayout databaseLayout;
    private UncloseableDelegatingFileSystemAbstraction uncloseableFs;

    @BeforeEach
    void initialize()
    {
        databaseLayout = testDirectory.databaseLayout();
        uncloseableFs = new UncloseableDelegatingFileSystemAbstraction( fs );
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setFileSystem( uncloseableFs )
                .impermanent()
                .build();
        GraphDatabaseService graphdb = managementService.database( DEFAULT_DATABASE_NAME );
        createInitialData( graphdb );
        managementService.shutdown();
        Map<String, String> params = new HashMap<>();
        params.put( GraphDatabaseSettings.rebuild_idgenerators_fast.name(), Settings.FALSE );
        Config config = Config.defaults( params );
        factory = new StoreFactory( databaseLayout, config, new DefaultIdGeneratorFactory( fs ), pageCache, fs, NullLogProvider.getInstance() );
    }

    @AfterEach
    void verifyAndDispose() throws Exception
    {
        GraphDatabaseService graphdb = null;
        DatabaseManagementService managementService = null;
        try
        {
            managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                    .setFileSystem( uncloseableFs )
                    .impermanent()
                    .build();
            graphdb = managementService.database( DEFAULT_DATABASE_NAME );
            verifyData( graphdb );
        }
        finally
        {
            if ( graphdb != null )
            {
                managementService.shutdown();
            }
            if ( fs != null )
            {
                fs.assertNoOpenFiles();
            }
        }
    }

    @Test
    void neostore()
    {
        performTest( databaseLayout.idMetadataStore() );
    }

    @Test
    void neostore_nodestore_db()
    {
        performTest( databaseLayout.idNodeStore() );
    }

    @Test
    void neostore_propertystore_db_arrays()
    {
        performTest( databaseLayout.idPropertyArrayStore() );
    }

    @Test
    void neostore_propertystore_db()
    {
        performTest( databaseLayout.idPropertyStore() );
    }

    @Test
    void neostore_propertystore_db_index()
    {
        performTest( databaseLayout.idPropertyKeyTokenStore() );
    }

    @Test
    void neostore_propertystore_db_index_keys()
    {
        performTest( databaseLayout.idPropertyKeyTokenNamesStore() );
    }

    @Test
    void neostore_propertystore_db_strings()
    {
        performTest( databaseLayout.idPropertyStringStore() );
    }

    @Test
    void neostore_relationshipstore_db()
    {
        performTest( databaseLayout.idRelationshipStore() );
    }

    @Test
    void neostore_relationshiptypestore_db()
    {
        performTest( databaseLayout.idRelationshipTypeTokenStore() );
    }

    @Test
    void neostore_relationshiptypestore_db_names()
    {
        performTest( databaseLayout.idRelationshipTypeTokenNamesStore() );
    }

    private void performTest( File idFile )
    {
        // emulate the need for rebuilding id generators by deleting it
        fs.deleteFile( idFile );
        try ( NeoStores neoStores = factory.openAllNeoStores() )
        {
            // close neoStores in case of failure
        }
        assertTrue( fs.fileExists( idFile ) );
    }

    private static void verifyData( GraphDatabaseService graphdb )
    {
        try ( Transaction tx = graphdb.beginTx() )
        {
            int nodecount = 0;
            for ( Node node : graphdb.getAllNodes() )
            {
                int propcount = readProperties( node );
                int relcount = 0;
                for ( Relationship rel : node.getRelationships() )
                {
                    assertEquals( 3, readProperties( rel ), "all relationships should have 3 properties." );
                    relcount++;
                }
                assertEquals( 3, propcount, "all created nodes should have 3 properties." );
                assertEquals( 2, relcount, "all created nodes should have 3 properties." );

                nodecount++;
            }
            assertEquals( 2, nodecount, "The database should have 2 nodes." );
        }
    }

    private static void createInitialData( GraphDatabaseService graphdb )
    {
        try ( Transaction tx = graphdb.beginTx() )
        {
            Node first = properties( graphdb.createNode() );
            Node other = properties( graphdb.createNode() );
            properties( first.createRelationshipTo( other, RelationshipType.withName( "KNOWS" ) ) );
            properties( other.createRelationshipTo( first, RelationshipType.withName( "DISTRUSTS" ) ) );

            tx.success();
        }
    }

    private static <E extends PropertyContainer> E properties( E entity )
    {
        entity.setProperty( "short thing", "short" );
        entity.setProperty( "long thing",
                "this is quite a long string, don't you think, it sure is long enough at least" );
        entity.setProperty( "string array", new String[]{"these are a few", "cool strings",
                "for your viewing pleasure"} );
        return entity;
    }

    private static int readProperties( PropertyContainer entity )
    {
        int count = 0;
        for ( String key : entity.getPropertyKeys() )
        {
            entity.getProperty( key );
            count++;
        }
        return count;
    }
}
