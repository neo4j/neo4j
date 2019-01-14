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
package org.neo4j.index.impl.lucene.explicit;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;

public class TestMigration
{
    @Rule
    public final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void providerGetsFilledInAutomatically()
    {
        Map<String, String> correctConfig = MapUtil.stringMap( "type", "exact", IndexManager.PROVIDER, "lucene" );
        File storeDir = testDirectory.storeDir();
        Neo4jTestCase.deleteFileOrDirectory( storeDir );
        GraphDatabaseService graphDb = startDatabase( storeDir );
        try ( Transaction transaction = graphDb.beginTx() )
        {
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index().forNodes( "default" ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration(
                    graphDb.index().forNodes( "wo-provider", MapUtil.stringMap( "type", "exact" ) ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index().forNodes( "w-provider",
                    MapUtil.stringMap( "type", "exact", IndexManager.PROVIDER, "lucene" ) ) ) );
            assertEquals( correctConfig,
                    graphDb.index().getConfiguration( graphDb.index().forRelationships( "default" ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration(
                    graphDb.index().forRelationships( "wo-provider", MapUtil.stringMap( "type", "exact" ) ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index()
                    .forRelationships( "w-provider",
                            MapUtil.stringMap( "type", "exact", IndexManager.PROVIDER, "lucene" ) ) ) );
            transaction.success();
        }

        graphDb.shutdown();

        removeProvidersFromIndexDbFile( testDirectory.databaseLayout() );
        graphDb = startDatabase( storeDir );

        try ( Transaction ignored = graphDb.beginTx() )
        {
            // Getting the index w/o exception means that the provider has been reinstated
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index().forNodes( "default" ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration(
                    graphDb.index().forNodes( "wo-provider", MapUtil.stringMap( "type", "exact" ) ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index().forNodes( "w-provider",
                    MapUtil.stringMap( "type", "exact", IndexManager.PROVIDER, "lucene" ) ) ) );
            assertEquals( correctConfig,
                    graphDb.index().getConfiguration( graphDb.index().forRelationships( "default" ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration(
                    graphDb.index().forRelationships( "wo-provider", MapUtil.stringMap( "type", "exact" ) ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index()
                    .forRelationships( "w-provider",
                            MapUtil.stringMap( "type", "exact", IndexManager.PROVIDER, "lucene" ) ) ) );
        }

        graphDb.shutdown();

        removeProvidersFromIndexDbFile( testDirectory.databaseLayout() );
        graphDb = startDatabase( storeDir );

        try ( Transaction ignored = graphDb.beginTx() )
        {
            // Getting the index w/o exception means that the provider has been reinstated
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index().forNodes( "default" ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index().forNodes( "wo-provider" ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index().forNodes( "w-provider" ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index().forRelationships( "default" ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index().forRelationships( "wo-provider" ) ) );
            assertEquals( correctConfig, graphDb.index().getConfiguration( graphDb.index().forRelationships( "w-provider" ) ) );
        }

        graphDb.shutdown();
    }

    private static GraphDatabaseService startDatabase( File storeDir )
    {
        return new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
    }

    private void removeProvidersFromIndexDbFile( DatabaseLayout databaseLayout )
    {
        IndexConfigStore indexStore = new IndexConfigStore( databaseLayout, fileSystemRule.get() );
        for ( Class<? extends PropertyContainer> cls : new Class[] {Node.class, Relationship.class} )
        {
            for ( String name : indexStore.getNames( cls ) )
            {
                Map<String, String> config = indexStore.get( cls, name );
                config = new HashMap<>( config );
                config.remove( IndexManager.PROVIDER );
                indexStore.set( Node.class, name, config );
            }
        }
    }
}
