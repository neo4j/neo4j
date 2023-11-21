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
package org.neo4j.kernel.api.impl.schema.sampler;

import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneIndexProvider;
import org.neo4j.kernel.api.impl.schema.TaskCoordinator;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProgressor;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexReader;
import org.neo4j.kernel.api.index.IndexSampler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexProxyAdapter;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingJob;
import org.neo4j.kernel.impl.api.index.sampling.OnlineIndexSamplingJobFactory;
import org.neo4j.kernel.impl.index.schema.PartitionedValueSeek;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.SlotSelector;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.logging.NullLogProvider.getInstance;

@EphemeralTestDirectoryExtension
class LuceneIndexSamplerReleaseTaskControlUnderFusionTest
{
    private static final int indexId = 1;
    private static final IndexDescriptor descriptor = IndexPrototype.forSchema( forLabel( 1, 1 ) ).withName( "index_1" ).materialise( indexId );
    private static final IndexProviderDescriptor providerDescriptor = IndexProviderDescriptor.UNDECIDED;
    private static final DirectoryFactory.InMemoryDirectoryFactory luceneDirectoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private static final Config config = Config.defaults();
    private static final IndexSamplingConfig samplingConfig = new IndexSamplingConfig( config );
    private static final RuntimeException sampleException = new RuntimeException( "Killroy messed with your index sample." );
    private static final TokenNameLookup tokenNameLookup = SIMPLE_NAME_LOOKUP;

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory dir;

    private IndexDirectoryStructure.Factory directoryFactory;

    @BeforeEach
    void setup()
    {
        directoryFactory = directoriesByProvider( dir.homePath() );
    }

    /**
     * This test come from a support case where dropping an index would block forever after index sampling failed.
     * <p>
     * A fusion index has multiple {@link IndexSampler index samplers} that are called sequentially. If one fails, then the other will never be invoked.
     * This was a problem for {@link LuceneIndexSampler}. It owns a {@link TaskCoordinator.Task} that it will try to release in try-finally
     * in {@link IndexSampler#sampleIndex(CursorContext, AtomicBoolean)}. But it never gets here because a prior {@link IndexSampler} fails.
     * <p>
     * Because the {@link TaskCoordinator.Task} was never released the lucene accessor would block forever, waiting for
     * {@link TaskCoordinator#awaitCompletion()}.
     * <p>
     * This situation was solved by making {@link IndexSampler} {@link java.io.Closeable} and include it in try-with-resource together with
     * {@link IndexReader} that created it.
     */
    @Test
    void failedIndexSamplingMustNotPreventIndexDrop() throws IOException, IndexEntryConflictException
    {
        LuceneIndexProvider luceneProvider = luceneProvider();
        makeSureIndexHasSomeData( luceneProvider ); // Otherwise no sampler will be created.

        IndexProvider failingProvider = failingProvider();
        FusionIndexProvider fusionProvider = createFusionProvider( luceneProvider, failingProvider );
        try ( IndexAccessor fusionAccessor = fusionProvider.getOnlineAccessor( descriptor, samplingConfig, tokenNameLookup ) )
        {
            IndexSamplingJob indexSamplingJob = createIndexSamplingJob( fusionAccessor );

            // Call run from other thread
            try
            {
                indexSamplingJob.run( new AtomicBoolean() );
            }
            catch ( RuntimeException e )
            {
                assertSame( sampleException, e );
            }

            // then
            fusionAccessor.drop();
            // should not block forever
        }
    }

    private static void makeSureIndexHasSomeData( IndexProvider provider ) throws IOException, IndexEntryConflictException
    {
        try ( IndexAccessor accessor = provider.getOnlineAccessor( descriptor, samplingConfig, tokenNameLookup );
              IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE, CursorContext.NULL ) )
        {
            updater.process( IndexEntryUpdate.add( 1, descriptor, Values.of( "some string" ) ) );
        }
    }

    private FusionIndexProvider createFusionProvider( LuceneIndexProvider luceneProvider, IndexProvider failingProvider )
    {
        SlotSelector slotSelector = SlotSelector.nullInstance;
        return new FusionIndexProvider( failingProvider, luceneProvider,
            slotSelector, providerDescriptor, directoryFactory, fs, false, writable() );
    }

    private static IndexSamplingJob createIndexSamplingJob( IndexAccessor fusionAccessor )
    {
        IndexProxyAdapter indexProxy = new IndexProxyAdapter()
        {
            @Override
            public IndexDescriptor getDescriptor()
            {
                return descriptor;
            }

            @Override
            public ValueIndexReader newValueReader()
            {
                return fusionAccessor.newValueReader();
            }
        };
        OnlineIndexSamplingJobFactory onlineIndexSamplingJobFactory = new OnlineIndexSamplingJobFactory( null, SIMPLE_NAME_LOOKUP, getInstance(),
                PageCacheTracer.NULL );
        return onlineIndexSamplingJobFactory.create( 1, indexProxy );
    }

    private LuceneIndexProvider luceneProvider()
    {
        return new LuceneIndexProvider( fs, luceneDirectoryFactory, directoryFactory, new Monitors(), config, writable() );
    }

    /**
     * @return an {@link IndexProvider} that create an {@link IndexAccessor} that create an {@link IndexReader} that create an {@link IndexSampler} that
     * throws exception... yeah.
     */
    private IndexProvider failingProvider()
    {
        return new IndexProvider.Adaptor( providerDescriptor, directoryFactory )
        {
            @Override
            public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, TokenNameLookup tokenNameLookup )
            {
                return failingIndexAccessor();
            }
        };
    }

    private static IndexAccessor failingIndexAccessor()
    {
        return new IndexAccessor.Adapter()
        {
            @Override
            public ValueIndexReader newValueReader()
            {
                return new ValueIndexReader()
                {
                    @Override
                    public void close()
                    {
                    }

                    @Override
                    public long countIndexedEntities( long entityId, CursorContext cursorContext, int[] propertyKeyIds, Value... propertyValues )
                    {
                        return 0;
                    }

                    @Override
                    public IndexSampler createSampler()
                    {
                        return ( cursorContext, stopped ) ->
                        {
                            throw sampleException;
                        };
                    }

                    @Override
                    public void query( IndexProgressor.EntityValueClient client, QueryContext context, AccessMode accessMode,
                                       IndexQueryConstraints constraints, PropertyIndexQuery... query )
                    {
                    }

                    @Override
                    public PartitionedValueSeek valueSeek( int desiredNumberOfPartitions, QueryContext context, PropertyIndexQuery... query )
                    {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }
}
