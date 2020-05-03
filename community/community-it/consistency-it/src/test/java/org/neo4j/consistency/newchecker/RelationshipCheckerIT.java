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
package org.neo4j.consistency.newchecker;

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.full.ConsistencyFlags;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.graphdb.schema.IndexType;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.index.label.RelationshipTypeScanStore;
import org.neo4j.internal.recordstorage.RecordStorageEngine;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.token.TokenHolders;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.consistency.newchecker.ParallelExecution.NOOP_EXCEPTION_HANDLER;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@DbmsExtension
class RelationshipCheckerIT
{
    private static final String INDEX_NAME = "index";
    private final ParallelExecution execution = new ParallelExecution( 10, NOOP_EXCEPTION_HANDLER, 100 );
    @Inject
    private GraphDatabaseAPI database;
    @Inject
    private RecordStorageEngine storageEngine;
    @Inject
    private IndexProviderMap providerMap;
    @Inject
    private Config config;
    @Inject
    private PageCache pageCache;
    @Inject
    private LabelScanStore labelScanStore;
    @Inject
    private RelationshipTypeScanStore relationshipTypeScanStore;
    @Inject
    private TokenHolders tokenHolders;
    private long relationshipId;
    private CheckerContext context;
    private DefaultPageCacheTracer pageCacheTracer;

    @BeforeEach
    void setUp() throws Exception
    {
        pageCacheTracer = new DefaultPageCacheTracer();
        var label = label( "any" );
        var type = withName( "type" );
        var propertyName = "property";
        try ( var tx = database.beginTx() )
        {
            var start = tx.createNode( label );
            var end = tx.createNode();
            var rel = start.createRelationshipTo( end, type );
            rel.setProperty( propertyName, "value" );
            relationshipId = rel.getId();
            tx.commit();
        }
        try ( var tx = database.beginTx() )
        {
            tx.schema().indexFor( type ).on( propertyName ).withName( INDEX_NAME ).withIndexType( IndexType.FULLTEXT ).create();
            tx.commit();
        }
        try ( var transaction = database.beginTx() )
        {
            transaction.schema().awaitIndexesOnline( 10, MINUTES );
        }

        prepareContext();
    }

    @Test
    void tracePageCacheAccessOnRelationshipCheck() throws Exception
    {
        prepareContext();
        var relationshipChecker = new RelationshipChecker( context, new IntObjectHashMap<>() );

        relationshipChecker.check( LongRange.range( 0, relationshipId + 1 ), true, false );

        assertThat( pageCacheTracer.pins() ).isEqualTo( 2 );
        assertThat( pageCacheTracer.unpins() ).isEqualTo( 2 );
        assertThat( pageCacheTracer.hits() ).isEqualTo( 2 );
    }

    private void prepareContext() throws Exception
    {
        var neoStores = storageEngine.testAccessNeoStores();
        var indexAccessors = new IndexAccessors( providerMap, neoStores, new IndexSamplingConfig( config ), PageCacheTracer.NULL );
        context = new CheckerContext( neoStores, indexAccessors, labelScanStore, relationshipTypeScanStore,
                execution, mock( ConsistencyReport.Reporter.class, RETURNS_MOCKS ), CacheAccess.EMPTY,
                tokenHolders, mock( RecordLoading.class ), mock( CountsState.class ), mock( NodeBasedMemoryLimiter.class ),
                ProgressMonitorFactory.NONE.multipleParts( "test" ), pageCache, pageCacheTracer, INSTANCE, false, ConsistencyFlags.DEFAULT );
        context.initialize();
    }
}
