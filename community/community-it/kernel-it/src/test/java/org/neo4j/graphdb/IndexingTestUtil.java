/*
 * Copyright (c) "Neo4j"
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

import java.util.function.Predicate;

import org.neo4j.graphdb.schema.IndexDefinition;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.graphdb.schema.IndexType.LOOKUP;

public class IndexingTestUtil
{
    public static void assertOnlyDefaultTokenIndexesExists( GraphDatabaseService db )
    {
        try ( var tx = db.beginTx() )
        {
            assertThat( stream( tx.schema().getIndexes().spliterator(), false ).count() ).isEqualTo( 2 );
            assertDefaultTokenIndexesExists( db );
        }
    }

    public static void assertDefaultTokenIndexesExists( GraphDatabaseService db )
    {
        try ( var tx = db.beginTx() )
        {
            var lookupIndexes = stream( tx.schema().getIndexes().spliterator(), false )
                    .filter( idx -> idx.getIndexType() == LOOKUP ).collect( toList() );
            assertThat( lookupIndexes.stream().anyMatch( IndexDefinition::isNodeIndex ) ).isTrue();
            assertThat( lookupIndexes.stream().anyMatch( IndexDefinition::isRelationshipIndex ) ).isTrue();
        }
    }

    public static void dropAllIndexes( GraphDatabaseService db )
    {
        dropIndexes( db, alwaysTrue() );
    }

    public static void dropTokenIndexes( GraphDatabaseService db )
    {
        dropIndexes( db, index -> index.isRelationshipIndex() || index.isNodeIndex() );
    }

    private static void dropIndexes( GraphDatabaseService db, Predicate<IndexDefinition> condition )
    {
        try ( var tx = db.beginTx() )
        {
            tx.schema().getIndexes().forEach( index ->
            {
                if ( condition.test( index ) )
                {
                    index.drop();
                }
            } );
            tx.commit();
        }
    }
}
