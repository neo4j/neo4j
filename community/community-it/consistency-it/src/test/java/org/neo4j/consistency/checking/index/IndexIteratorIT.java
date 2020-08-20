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
package org.neo4j.consistency.checking.index;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.internal.helpers.collection.Iterators.count;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;

@DbmsExtension
class IndexIteratorIT
{
    private static final String INDEX_NAME = "index";
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;
    @Inject
    private IndexProviderMap providerMap;
    @Inject
    private Config config;

    private IndexAccessors indexAccessors;
    private DefaultPageCacheTracer pageCacheTracer;

    @BeforeEach
    void setUp() throws IOException
    {
        var label = label( "any" );
        var propertyName = "property";
        try ( var tx = database.beginTx() )
        {
            var node = tx.createNode( label );
            node.setProperty( propertyName, "nodeValue" );
            tx.commit();
        }
        try ( var tx = database.beginTx() )
        {
            tx.schema().indexFor( label ).on( propertyName ).withName( INDEX_NAME ).create();
            tx.commit();
        }
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 10, MINUTES );
        }

        var neoStores = storageEngine.testAccessNeoStores();
        indexAccessors = new IndexAccessors( providerMap, neoStores, new IndexSamplingConfig( config ), PageCacheTracer.NULL, SIMPLE_NAME_LOOKUP );
        pageCacheTracer = new DefaultPageCacheTracer();
    }

    @Test
    void tracePageCacheAccessOnIteration() throws Exception
    {
        var descriptors = indexAccessors.onlineRules();
        assertThat( descriptors ).hasSize( 1 );
        for ( IndexDescriptor descriptor : descriptors )
        {
            try ( IndexIterator indexIterator = new IndexIterator( indexAccessors.accessorFor( descriptor ), pageCacheTracer ) )
            {
                assertEquals( 1, count( indexIterator.iterator() ) );
            }
        }

        assertThat( pageCacheTracer.pins() ).isOne();
        assertThat( pageCacheTracer.unpins() ).isOne();
        assertThat( pageCacheTracer.hits() ).isOne();
    }
}
