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
package org.neo4j.kernel;

import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;

@DbmsExtension
class TransactionalQueryExecutionIT
{
    @Inject
    private GraphDatabaseService db;

    @Test
    void executeQueryTransactionally()
    {
        final Label marker = Label.label( "MARKER" );
        assertEquals( 0L, countMarkedNodes( marker ) );
        db.executeTransactionally( "CREATE (n:MARKER)" );
        db.executeTransactionally( "CREATE (n:MARKER)" );
        db.executeTransactionally( "CREATE (n:MARKER)" );
        assertEquals( 3L, countMarkedNodes( marker ) );
    }

    @Test
    void executeQueryAndConsumeResult()
    {
        db.executeTransactionally( "CREATE (n:CONSUMABLE)" );
        db.executeTransactionally( "CREATE (n:CONSUMABLE)" );
        db.executeTransactionally( "CREATE (n:CONSUMABLE)" );
        db.executeTransactionally( "CREATE (n:CONSUMABLE)" );
        var countingResultVisitor = new CountingResultVisitor();
        db.executeTransactionally( "MATCH (n:CONSUMABLE) RETURN n", countingResultVisitor );
        assertEquals( 4L, countingResultVisitor.getNodeCounter() );
    }

    private long countMarkedNodes( Label marker )
    {
        try ( var transaction = db.beginTx() )
        {
            return Iterators.count( db.findNodes( marker ) );
        }
    }

    private static class CountingResultVisitor implements Result.ResultVisitor<RuntimeException>
    {
        private int nodeCounter;

        @Override
        public boolean visit( Result.ResultRow row )
        {
            if ( row.get( "n" ) != null )
            {
                nodeCounter++;
            }
            return true;
        }

        int getNodeCounter()
        {
            return nodeCounter;
        }
    }
}
