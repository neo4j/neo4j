/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package common;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public abstract class AbstractTestBase
{
    private static GraphDatabaseService graphdb;
    private static DatabaseManagementService managementService;

    @BeforeClass
    public static void beforeSuite()
    {
        managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        graphdb = managementService.database( DEFAULT_DATABASE_NAME );
    }

    @AfterClass
    public static void afterSuite()
    {
        managementService.shutdown();
        graphdb = null;
    }

    protected static Node getNode( Transaction tx, long id )
    {
        return tx.getNodeById( id );
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

        @Override
        public String represent( Relationship item )
        {
            return nodes.represent( item.getStartNode() ) + ' '
                   + rel.represent( item ) + ' '
                   + nodes.represent( item.getEndNode() );
        }
    }

    protected static <T> void expect( Iterable<? extends T> items,
            Representation<T> representation, String... expected )
    {
        expect( items, representation, new HashSet<>( Arrays.asList( expected ) ) );
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
            tx.commit();
        }

        if ( !expected.isEmpty() )
        {
            fail( "The expected elements " + expected + " were not returned." );
        }
    }
}
