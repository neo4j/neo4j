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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.Iterables.count;

public class RelationshipGroupStoreIT
{
    private final TargetDirectory.TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    private final CleanupRule cleanupRule = new CleanupRule();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( directory ).around( cleanupRule );

    @Test
    public void shouldCreateAllTheseRelationshipTypes() throws Exception
    {
        GraphDatabaseService db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( directory.graphDbDir().getPath() )
                .setConfig( GraphDatabaseSettings.dense_node_threshold, "1" )
                .newGraphDatabase();
        cleanupRule.add( db );

        shiftHighId( (GraphDatabaseAPI) db );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            for ( int i = 0; i < 5_000; i++ )
            {
                node.createRelationshipTo( db.createNode(), type( i + 1 ) );
            }
            tx.success();
        }

        int nodeWithHighRelationshipTypeID = 4001;
        try ( Transaction ignored = db.beginTx() )
        {
            Node node = db.getNodeById( 0 );
            assertEquals( "Should be possible to get relationships of type with id in unsigned short range.",
                    1, count( node.getRelationships( type( nodeWithHighRelationshipTypeID ) ) ) );
        }
    }

    private void shiftHighId( GraphDatabaseAPI db )
    {
        GraphDatabaseAPI databaseAPI = db;
        NeoStores neoStores = databaseAPI.getDependencyResolver().resolveDependency( NeoStores.class );
        neoStores.getRelationshipTypeTokenStore().setHighId( 30000 );
    }

    private DynamicRelationshipType type( int i )
    {
        return DynamicRelationshipType.withName( "TYPE_" + i );
    }

}
