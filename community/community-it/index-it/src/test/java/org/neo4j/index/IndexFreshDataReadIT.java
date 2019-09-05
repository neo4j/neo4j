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
package org.neo4j.index;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

@ImpermanentDbmsExtension
class IndexFreshDataReadIT
{
    @Inject
    private GraphDatabaseService db;

    private ExecutorService executor = Executors.newCachedThreadPool();

    @AfterEach
    void tearDown()
    {
        executor.shutdown();
    }

    @Test
    void readLatestIndexDataAfterUsingExhaustedNodeRelationshipIterator() throws Exception
    {
        try ( Transaction transaction = db.beginTx() )
        {
            addStaffMember( "Fry" );
            assertEquals( 1, countStaff( transaction ).intValue() );

            Node fry = transaction.getNodeById( 0 );
            Iterable<Relationship> fryRelationships = fry.getRelationships();
            assertFalse( fryRelationships.iterator().hasNext() );

            addStaffMember( "Lila" );
            assertEquals( 2, countStaff( transaction ).intValue() );

            addStaffMember( "Bender" );
            assertEquals( 3, countStaff( transaction ).intValue() );
        }
    }

    private void addStaffMember( String name ) throws InterruptedException, java.util.concurrent.ExecutionException
    {
        executor.submit( new CreateNamedNodeTask( name ) ).get();
    }

    private Number countStaff( Transaction tx )
    {
        try ( Result countResult = tx.execute( "MATCH (n:staff) return count(n.name) as count" ) )
        {
            return (Number) countResult.columnAs( "count" ).next();
        }
    }

    private class CreateNamedNodeTask implements Runnable
    {
        private final String name;

        CreateNamedNodeTask( String name )
        {
            this.name = name;
        }

        @Override
        public void run()
        {
            try ( Transaction transaction = db.beginTx() )
            {
                transaction.execute( "CREATE (n:staff {name:$name})", map( "name", name ) );
                transaction.commit();
            }
        }
    }
}
