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
package org.neo4j.kernel.impl.storemigration.legacystore.indexcompat;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.MultipleFoundException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.Unzip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.asList;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

@Ignore( "This test is for an index format change between 2.0.0 and 2.0.x so not applicable for later versions" )
public class IndexFormatCompatibilityTest
{
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    private GraphDatabaseService db;

    @Before
    public void startDatabase() throws IOException
    {
        File storeDir = Unzip.unzip( getClass(), "db.zip", testDirectory.graphDbDir() );
        db = new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir );
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
                    externalIds( db.findNodes( label( "Person" ), "age", 0 ) ) );
            assertEquals( asSet( 0 ),
                    externalIds( db.findNodes( label( "Person" ), "age", 1 ) ) );
            assertEquals( asSet( 1, 4 ),
                    externalIds( db.findNodes( label( "Person" ), "age", 2 ) ) );
            assertEquals( asSet( 2, 5, 7 ),
                    externalIds( db.findNodes( label( "Person" ), "age", 3 ) ) );
            assertEquals( asSet( 3, 6, 8, 9 ),
                    externalIds( db.findNodes( label( "Person" ), "age", 4 ) ) );

            tx.success();
        }
    }

    @Test( expected = MultipleFoundException.class )
    public void shouldThrowWhenMulitpleResultsForSingleNode() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.findNode( label( "Person" ), "age", 4 );
        }
    }

    @Test
    public void shouldFindCorrectNodesUsingUniquePropertyLookup() throws Exception
    {
        try ( Transaction tx = db.beginTx() )
        {
            assertEquals( 1, age( db.findNode( label( "Person" ), "externalId", 0 ) ) );
            assertEquals( 2, age( db.findNode( label( "Person" ), "externalId", 1 ) ) );
            assertEquals( 3, age( db.findNode( label( "Person" ), "externalId", 2 ) ) );
            assertEquals( 4, age( db.findNode( label( "Person" ), "externalId", 3 ) ) );
            assertEquals( 2, age( db.findNode( label( "Person" ), "externalId", 4 ) ) );
            assertTrue( asList( db.findNodes( label( "Person" ), "externalId", 10 ) ).isEmpty() );
            assertEquals( null, db.findNode( label( "Person" ), "externalId", 10 ) );

            tx.success();
        }
    }

    private Set<Integer> externalIds( Iterator<Node> nodes )
    {
        HashSet<Integer> externalIds = new HashSet<>();
        for ( Node node : loop( nodes ) )
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
