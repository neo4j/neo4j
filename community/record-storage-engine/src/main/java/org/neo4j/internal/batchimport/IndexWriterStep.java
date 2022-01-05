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
package org.neo4j.internal.batchimport;

import java.util.Iterator;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.common.EntityType;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.batchimport.staging.ProcessorStep;
import org.neo4j.internal.batchimport.staging.StageControl;
import org.neo4j.internal.batchimport.store.BatchingNeoStores;
import org.neo4j.internal.recordstorage.SchemaRuleAccess;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;

import static org.apache.commons.lang3.StringUtils.defaultIfEmpty;
import static org.neo4j.internal.helpers.collection.Iterators.stream;
import static org.neo4j.internal.recordstorage.SchemaRuleAccess.getSchemaRuleAccess;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexType.LOOKUP;
import static org.neo4j.internal.schema.SchemaDescriptors.forAnyEntityTokens;
import static org.neo4j.internal.schema.SchemaNameUtil.generateName;

public abstract class IndexWriterStep<T> extends ProcessorStep<T>
{
    private static final String INDEX_IMPORTER_CREATION_TAG = "indexImporterCreation";

    public IndexWriterStep( StageControl control, String name, Configuration config, int maxProcessors, PageCacheTracer pageCacheTracer )
    {
        super( control, name, config, maxProcessors, pageCacheTracer );
    }

    protected IndexImporter indexImporter(
            IndexConfig indexConfig, IndexImporterFactory importerFactory, BatchingNeoStores neoStores, EntityType entityType,
            MemoryTracker memoryTracker, CursorContextFactory contextFactory, Function<CursorContext,StoreCursors> storeCursorsFactory )
    {
        var schemaStore = neoStores.getNeoStores().getSchemaStore();
        var metaDataStore = neoStores.getNeoStores().getMetaDataStore();
        var tokenHolders = neoStores.getTokenHolders();
        var schemaRuleAccess = getSchemaRuleAccess( schemaStore, tokenHolders, metaDataStore );
        try ( var cursorContext = contextFactory.create( INDEX_IMPORTER_CREATION_TAG );
                var storeCursors = storeCursorsFactory.apply( cursorContext ) )
        {
            var index = findIndex( entityType, schemaRuleAccess, storeCursors ).orElseGet(
                    () -> createIndex( entityType, indexConfig, schemaRuleAccess, schemaStore, memoryTracker, cursorContext, storeCursors ) );
            return importerFactory.getImporter( index, neoStores.databaseLayout(), neoStores.fileSystem(), neoStores.getPageCache(), contextFactory );
        }
    }

    private static IndexDescriptor createIndex(
            EntityType entityType, IndexConfig config, SchemaRuleAccess schemaRule, SchemaStore schemaStore,
            MemoryTracker memoryTracker, CursorContext cursorContext, StoreCursors storeCursors )
    {
        try
        {
            IndexProviderDescriptor providerDescriptor = new IndexProviderDescriptor( "token-lookup", "1.0" );
            IndexPrototype prototype = forSchema( forAnyEntityTokens( entityType ) ).withIndexType( LOOKUP ).withIndexProvider( providerDescriptor );
            String name = defaultIfEmpty( config.indexName( entityType ), generateName( prototype, new String[]{}, new String[]{} ) );
            IndexDescriptor descriptor = prototype.withName( name ).materialise( schemaStore.nextId( cursorContext ) );
            schemaRule.writeSchemaRule( descriptor, cursorContext, memoryTracker, storeCursors );
            return descriptor;
        }
        catch ( KernelException e )
        {
            throw new RuntimeException( "Error preparing indexes", e );
        }
    }

    private static Optional<IndexDescriptor> findIndex( EntityType entityType, SchemaRuleAccess schemaRule, StoreCursors storeCursors )
    {
        Iterator<IndexDescriptor> descriptors = schemaRule.indexesGetAll( storeCursors );
        return stream( descriptors ).filter( index -> index.schema().entityType() == entityType && index.isTokenIndex() ).findFirst();
    }
}
