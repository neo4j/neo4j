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

import java.util.Collections;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.ResultTransformer;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.internal.helpers.collection.MapUtil.map;

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
        assertEquals( 4, db.executeTransactionally( "MATCH (n:CONSUMABLE) RETURN n", Collections.emptyMap(), new CountingResultTransformer() ) );
    }

    @Test
    void executeQueryWithParametersTransactionally()
    {
        db.executeTransactionally( "CREATE (n:NODE) SET n = $data RETURN n", map( "data", map( "key", "value" ) ) );

        try ( var transaction = db.beginTx() )
        {
            assertNotNull( transaction.findNode( Label.label( "NODE" ), "key", "value" ) );
        }
    }

    private long countMarkedNodes( Label marker )
    {
        try ( var transaction = db.beginTx() )
        {
            return Iterators.count( transaction.findNodes( marker ) );
        }
    }

    private static class CountingResultTransformer implements ResultTransformer<Integer>
    {
        @Override
        public Integer apply( Result result )
        {
            int nodeCounter = 0;
            while ( result.hasNext() )
            {
                var row = result.next();
                if ( row.get( "n" ) != null )
                {
                    nodeCounter++;
                }
            }
            return nodeCounter;
        }
    }
}
