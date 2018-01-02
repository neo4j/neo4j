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
package org.neo4j.kernel.impl.traversal;

import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static org.neo4j.graphdb.traversal.Evaluators.includeWhereLastRelationshipTypeIs;
import static org.neo4j.kernel.Traversal.path;
import static org.neo4j.kernel.Traversal.traversal;
import static org.neo4j.kernel.Uniqueness.NODE_PATH;

public class TestPathDescription extends TraversalTestBase
{
    private static final RelationshipType A = DynamicRelationshipType.withName( "A" );
    private static final RelationshipType B = DynamicRelationshipType.withName( "B" );
    private static final RelationshipType C = DynamicRelationshipType.withName( "C" );
    private static final RelationshipType D = DynamicRelationshipType.withName( "D" );

    @Test
    public void specificPath() throws Exception
    {
        /**
         *    (1) -A-> (2) -B-> (3) -C-> (4) -D-> (5)
         *      \              /
         *       A    ---B-----
         *        v  /
         *        (6)
         */
        createGraph(
                "1 A 2", "2 B 3", "3 C 4", "4 D 5",
                "1 A 6", "6 B 3" );

        try ( Transaction tx = beginTx() )
        {
            expectPaths( traversal( NODE_PATH )
                    .expand( path().step( A ).step( B ).step( C ).step( D ).build() )
                    .evaluator( includeWhereLastRelationshipTypeIs( D ) )
                    .traverse( getNodeWithName( "1" ) ),
                    "1,2,3,4,5", "1,6,3,4,5" );
        }
    }
}
