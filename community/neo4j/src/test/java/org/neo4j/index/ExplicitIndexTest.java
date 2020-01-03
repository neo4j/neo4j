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
package org.neo4j.index;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Map;
import java.util.stream.IntStream;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import static java.time.Duration.ofMillis;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

@ExtendWith( TestDirectoryExtension.class )
class ExplicitIndexTest
{
    private static final long TEST_TIMEOUT = 80_000;

    @Inject
    private TestDirectory directory;

    @Test
    void explicitIndexPopulationWithBunchOfFields()
    {
        assertTimeout( ofMillis( TEST_TIMEOUT ), () ->
        {
            BatchInserter batchNode = BatchInserters.inserter( directory.databaseDir() );
            LuceneBatchInserterIndexProvider provider = new LuceneBatchInserterIndexProvider( batchNode );
            try
            {
                BatchInserterIndex batchIndex = provider.nodeIndex( "node_auto_index", stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );

                Map<String,Object> properties = IntStream.range( 0, 2000 )
                        .mapToObj( i -> Pair.of( Integer.toString( i ), randomAlphabetic( 200 ) ) )
                        .collect( toMap( Pair::first, Pair::other ) );

                long node = batchNode.createNode( properties, Label.label( "NODE" ) );
                batchIndex.add( node, properties );
            }
            finally
            {
                provider.shutdown();
                batchNode.shutdown();
            }
        } );
    }
}
