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
package common;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class AbstractTestBase
{
    private static GraphDatabaseService graphdb;

    @BeforeClass
    public static void beforeSuite()
    {
        graphdb = new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @AfterClass
    public static void afterSuite()
    {
        graphdb.shutdown();
        graphdb = null;
    }

    protected static Node getNode( long id )
    {
        return graphdb.getNodeById( id );
    }

    protected static Transaction beginTx()
    {
        return graphdb.beginTx();
    }

    protected interface Representation<T>
    {
        String represent( T item );
    }

    protected static final class RelationshipRepresentation implements
            Representation<Relationship>
    {
        private final Representation<? super Node> nodes;
        private final Representation<? super Relationship> rel;

        public RelationshipRepresentation( Representation<? super Node> nodes,
                Representation<? super Relationship> rel )
        {
            this.nodes = nodes;
            this.rel = rel;
        }

        public String represent( Relationship item )
        {
            return nodes.represent( item.getStartNode() ) + " "
                   + rel.represent( item ) + " "
                   + nodes.represent( item.getEndNode() );
        }
    }

    protected static <T> void expect( Iterable<? extends T> items,
            Representation<T> representation, String... expected )
    {
        expect( items, representation, new HashSet<String>(
                Arrays.asList( expected ) ) );
    }

    protected static <T> void expect( Iterable<? extends T> items,
            Representation<T> representation, Set<String> expected )
    {
        try ( Transaction tx = beginTx() )
        {
            for ( T item : items )
            {
                String repr = representation.represent( item );
                assertTrue( repr + " not expected ", expected.remove( repr ) );
            }
            tx.success();
        }

        if ( !expected.isEmpty() )
        {
            fail( "The exepected elements " + expected + " were not returned." );
        }
    }
}
