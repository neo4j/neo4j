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
package org.neo4j.index;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.EmbeddedDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.helpers.collection.MapUtil.map;

public class IndexFreshDataReadIT
{
    @Rule
    public EmbeddedDatabaseRule databaseRule = new EmbeddedDatabaseRule();

    private ExecutorService executor = Executors.newCachedThreadPool();

    @After
    public void setUp() throws Exception
    {
        executor.shutdown();
    }

    @Test
    public void readLatestIndexDataAfterUsingExhaustedNodeRelationshipIterator() throws Exception
    {
        try ( Transaction transaction = databaseRule.beginTx() )
        {
            addStaffMember( "Fry" );
            assertEquals( 1, countStaff().intValue() );

            Node fry = databaseRule.getNodeById( 0 );
            Iterable<Relationship> fryRelationships = fry.getRelationships();
            assertFalse( fryRelationships.iterator().hasNext() );

            addStaffMember( "Lila" );
            assertEquals( 2, countStaff().intValue() );

            addStaffMember( "Bender" );
            assertEquals( 3, countStaff().intValue() );
        }
    }

    private void addStaffMember( String name ) throws InterruptedException, java.util.concurrent.ExecutionException
    {
        executor.submit( new CreateNamedNodeTask( name ) ).get();
    }

    private Number countStaff()
    {
        try ( Result countResult = databaseRule.execute( "MATCH (n:staff) return count(n.name) as count" ) )
        {
            return (Number) countResult.columnAs( "count" ).next();
        }
    }

    private class CreateNamedNodeTask implements Runnable
    {
        private final String name;

        CreateNamedNodeTask(String name)
        {
            this.name = name;
        }

        @Override
        public void run()
        {
            try ( Transaction transaction = databaseRule.beginTx() )
            {
                databaseRule.execute( "CREATE (n:staff {name:{name}})", map( "name", name ) );
                transaction.success();
            }
        }
    }
}
