/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

public class TransactionLifecycleTest
{
    @Rule
    public DatabaseRule database = new ImpermanentDatabaseRule();

    @Test
    public void givenACallToFailATransactionSubsequentSuccessCallsShouldBeSwallowedSilently()
    {
        GraphDatabaseService graphdb = database.getGraphDatabaseService();
        Transaction tx = graphdb.beginTx();
        try
        {
            graphdb.createNode();
            tx.failure();

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
