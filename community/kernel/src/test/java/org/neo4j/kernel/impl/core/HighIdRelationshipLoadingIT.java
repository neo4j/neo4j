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
package org.neo4j.kernel.impl.core;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.Pair;
import org.neo4j.test.IdJumpingGraphDatabase;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.relationship_grab_size;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class HighIdRelationshipLoadingIT
{
    private final int
        ISLAND_SIZE = 188, //RANDOM.nextInt( 100 )+100,
        GRAB_SIZE = 100; //RANDOM.nextInt( ISLAND_SIZE*2 );

    @Test
    public void loadStuffWithHighIdsWhileIterating() throws Exception
    {
        // GIVEN -- a node with relationships of mixed ids, some below 2^32-1 and some above.
        int rels = GRAB_SIZE*2;
        Pair<Node, Set<Relationship>> data = createNodeWithRelationships( rels );
        Node node = data.first();

        // WHEN -- loading those relationships.
        clearCache();
        Set<Relationship> loadedRelationships = new HashSet<Relationship>();
        for ( Relationship relationship : node.getRelationships() )
        {
            assertTrue( loadedRelationships.add( relationship ) );
        }

        // THEN -- they should all be found in the iterator loading them.
        assertEquals( data.other(), loadedRelationships );
    }

    private void clearCache()
    {
        db.getDependencyResolver().resolveDependency( NodeManager.class ).clearCache();
    }

    private Pair<Node,Set<Relationship>> createNodeWithRelationships( int rels )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            Set<Relationship> relationships = new HashSet<Relationship>();
            for ( int i = 0; i < rels; i++ )
            {
                relationships.add( node.createRelationshipTo( db.createNode(), withName( "A" ) ) );
            }
            tx.success();
            return Pair.of( node, relationships );
        }
        finally
        {
            tx.finish();
        }
    }

    private final String storeDir = TargetDirectory.forTest( getClass() ).graphDbDir( true ).getAbsolutePath();
    private IdJumpingGraphDatabase db;

    @Before
    public void before() throws Exception
    {
        db = new IdJumpingGraphDatabase( storeDir,
                stringMap( relationship_grab_size.name(), "" + GRAB_SIZE ), ISLAND_SIZE );
    }

    @After
    public void after() throws Exception
    {
        db.shutdown();
    }
}