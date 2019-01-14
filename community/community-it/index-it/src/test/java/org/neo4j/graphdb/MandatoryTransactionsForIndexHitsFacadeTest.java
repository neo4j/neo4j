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
package org.neo4j.graphdb;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import static org.junit.Assert.fail;

public class MandatoryTransactionsForIndexHitsFacadeTest
{
    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private IndexHits<Node> indexHits;

    @Before
    public void before()
    {
        Index<Node> index = createIndex();
        indexHits = queryIndex( index );
    }

    @Test
    public void shouldMandateTransactionsForUsingIterator()
    {
        try ( ResourceIterator<Node> iterator = indexHits.iterator() )
        {
            try
            {
                iterator.hasNext();

                fail( "Transactions are mandatory, also for reads" );
            }
            catch ( NotInTransactionException e )
            {   // Expected
            }

            try
            {
                iterator.next();

                fail( "Transactions are mandatory, also for reads" );
            }
            catch ( NotInTransactionException e )
            {   // Expected
            }
        }
    }

    @Test
    public void shouldMandateTransactionsForGetSingle()
    {
        try
        {
            indexHits.getSingle();

            fail( "Transactions are mandatory, also for reads" );
        }
        catch ( NotInTransactionException e )
        {   // Expected
        }
    }

    private Index<Node> createIndex()
    {
        GraphDatabaseService graphDatabaseService = dbRule.getGraphDatabaseAPI();
        try ( Transaction transaction = graphDatabaseService.beginTx() )
        {
            Index<Node> index = graphDatabaseService.index().forNodes( "foo" );
            transaction.success();
            return index;
        }
    }

    private IndexHits<Node> queryIndex( Index<Node> index )
    {
        GraphDatabaseService graphDatabaseService = dbRule.getGraphDatabaseAPI();
        try ( Transaction ignored = graphDatabaseService.beginTx() )
        {
            IndexHits<Node> hits = index.get( "foo", 42 );
            hits.close();
            return hits;
        }
    }
}
