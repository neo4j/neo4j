/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.restore;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Optional;

import org.neo4j.coreedge.convert.ClusterSeed;
import org.neo4j.coreedge.convert.StoreMetadata;
import org.neo4j.coreedge.server.StoreId;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.logging.NullLog;
import org.neo4j.server.configuration.ConfigLoader;
import org.neo4j.test.rule.TargetDirectory;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.neo4j.coreedge.convert.GenerateClusterSeedCommand.storeId;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_ID;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.UPGRADE_TIME;
import static org.neo4j.kernel.impl.store.MetaDataStore.getRecord;
import static org.neo4j.restore.RestoreExistingClusterCli.settings;

public class RestoreClusterCliTest
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void shouldRestoreDatabase() throws Throwable
    {
        File classicDatabaseDir = testDirectory.cleanDirectory( "classic-db" );
        File classicNeo4jStore = createClassicNeo4jStore( classicDatabaseDir, 10, StandardV3_0.NAME );
        StoreMetadata storeMetadata = metadataFor( classicNeo4jStore );

        // when
        File homeDir = testDirectory.cleanDirectory( "new-db-1" );
        LinkedList<String> args = ArgsBuilder.args().homeDir( homeDir ).config( homeDir )
                .from( classicNeo4jStore ).database( "graph.db" ).build() ;

        String out = RestoreClusterUtils.execute( () -> RestoreNewClusterCli.main( args.toArray( new String[args.size()] ) ) );

        // then
        String seed = extractSeed( out );
        ClusterSeed clusterSeed = ClusterSeed.create( seed );

        assertTrue( storeMetadata.storeId().equals( clusterSeed.before() ) );
        assertEquals( storeMetadata.lastTxId(), clusterSeed.lastTxId() );
        assertFalse( storeMetadata.storeId().equals( clusterSeed.after() ) );

        // when restore to another place
        File rootNewDatabaseDir = testDirectory.cleanDirectory( "new-db-2" );
        LinkedList<String> newArgs = ArgsBuilder.args().homeDir( rootNewDatabaseDir ).config( rootNewDatabaseDir )
                .from( classicNeo4jStore ).database( "graph.db" ).seed( seed ).build() ;

        RestoreClusterUtils.execute( () -> RestoreExistingClusterCli.main( newArgs.toArray( new String[newArgs.size()] ) ) );

        // then
        StoreMetadata newMetadata = metadataFor( extractDatabaseDir( rootNewDatabaseDir ) );
        assertTrue( clusterSeed.after().equals( newMetadata.storeId() ) );
    }

    private File extractDatabaseDir( File rootNewDatabaseDir )
    {
        Config config = new ConfigLoader( settings() ).loadConfig( Optional.of( rootNewDatabaseDir ),
                Optional.of( new File( rootNewDatabaseDir, "neo4j.conf" ) ), NullLog.getInstance() );
        return config.get( DatabaseManagementSystemSettings.database_path );
    }

    private StoreMetadata metadataFor( File classicNeo4jStore ) throws IOException
    {
        FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
        File metadataStore = new File( classicNeo4jStore, MetaDataStore.DEFAULT_NAME );

        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fs ) )
        {
            StoreId classicStoreId = storeId( metadataStore, pageCache, getRecord( pageCache, metadataStore,
                    UPGRADE_TIME ) );
            long lastTransactionId = getRecord( pageCache, metadataStore, LAST_TRANSACTION_ID );
            return new StoreMetadata( classicStoreId, lastTransactionId );
        }
    }

    public static String extractSeed( String message )
    {
        return message.replace( "Cluster Seed: ", "" ).trim();
    }

    private File createClassicNeo4jStore( File base, int nodesToCreate, String recordFormat )
    {
        File existingDbDir = new File( base, "existing" );
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( existingDbDir )
                .setConfig( GraphDatabaseSettings.record_format, recordFormat )
                .newGraphDatabase();

        for ( int i = 0; i < (nodesToCreate / 2); i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                Node node1 = db.createNode( Label.label( "Label-" + i ) );
                Node node2 = db.createNode( Label.label( "Label-" + i ) );
                node1.createRelationshipTo( node2, RelationshipType.withName( "REL-" + i ) );
                tx.success();
            }
        }

        db.shutdown();

        return existingDbDir;
    }

}
