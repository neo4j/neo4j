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
package org.neo4j.kernel.impl.storemigration.legacystore.indexcompat;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.Unzip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class IndexFormatCompatibilityTest
{
    private GraphDatabaseService db;

    @Before
    public void startDatabase() throws IOException
    {
        File storeDir = Unzip.unzip( getClass(), "indexcompat.zip" );

        db = new GraphDatabaseFactory().newEmbeddedDatabase( storeDir.getPath() );
    }

    @After
    public void shutdownDatabase()
    {
        db.shutdown();
    }

    @Test
    public void shouldFindCorrectNodesUsingIndexedPropertyLookup() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( IteratorUtil.<Integer>asSet(),
                    externalIds( db.findNodesByLabelAndProperty( label( "Person" ), "age", 0 ) ) );
            assertEquals( asSet( 0 ),
                    externalIds( db.findNodesByLabelAndProperty( label( "Person" ), "age", 1 ) ) );
            assertEquals( asSet( 1, 4 ),
                    externalIds( db.findNodesByLabelAndProperty( label( "Person" ), "age", 2 ) ) );
            assertEquals( asSet( 2, 5, 7 ),
                    externalIds( db.findNodesByLabelAndProperty( label( "Person" ), "age", 3 ) ) );
            assertEquals( asSet( 3, 6, 8, 9 ),
                    externalIds( db.findNodesByLabelAndProperty( label( "Person" ), "age", 4 ) ) );

            tx.success();
        }
    }

    @Test
    public void shouldFindCorrectNodesUsingUniquePropertyLookup() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, age(single( db.findNodesByLabelAndProperty( label( "Person" ), "externalId", 0 ) ) ) );
            assertEquals( 2, age(single( db.findNodesByLabelAndProperty( label( "Person" ), "externalId", 1 ) ) ) );
            assertEquals( 3, age(single( db.findNodesByLabelAndProperty( label( "Person" ), "externalId", 2 ) ) ) );
            assertEquals( 4, age(single( db.findNodesByLabelAndProperty( label( "Person" ), "externalId", 3 ) ) ) );
            assertEquals( 2, age(single( db.findNodesByLabelAndProperty( label( "Person" ), "externalId", 4 ) ) ) );
            assertTrue( asList( db.findNodesByLabelAndProperty( label( "Person" ), "externalId", 10 ) ).isEmpty() );

            tx.success();
        }
    }

    private Set<Integer> externalIds( Iterable<Node> nodes )
    {
        HashSet<Integer> externalIds = new HashSet<>();
        for ( Node node : nodes )
        {
            externalIds.add( ((Number) node.getProperty( "externalId" )).intValue() );
        }
        return externalIds;
    }

    private int age( Node node )
    {
        return ((Number) node.getProperty( "age" )).intValue();
    }
}
