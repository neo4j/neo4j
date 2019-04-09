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
package org.neo4j.kernel.api.impl.schema.sampler;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.helpers.TaskCoordinator;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.impl.schema.LuceneIndexProvider;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexProxyAdapter;
import org.neo4j.kernel.impl.api.index.IndexStoreView;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingJob;
import org.neo4j.kernel.impl.api.index.sampling.OnlineIndexSamplingJobFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.schema.fusion.FusionIndexProvider;
import org.neo4j.kernel.impl.index.schema.fusion.SlotSelector;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.storageengine.api.schema.IndexSampler;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertSame;
import static org.neo4j.internal.kernel.api.IndexCapability.NO_CAPABILITY;
import static org.neo4j.kernel.api.impl.schema.LuceneIndexProvider.defaultDirectoryStructure;
import static org.neo4j.kernel.api.index.IndexProvider.EMPTY;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.simpleNameLookup;
import static org.neo4j.logging.NullLogProvider.getInstance;
import static org.neo4j.storageengine.api.schema.IndexDescriptorFactory.forSchema;

public class LuceneIndexSamplerReleaseTaskControlUnderFusion
{
    @Rule
    public FileSystemRule fs = new EphemeralFileSystemRule();

    @Rule
    public TestDirectory dir = TestDirectory.testDirectory( fs.get() );

    private static final int indexId = 1;
    private static final StoreIndexDescriptor storeIndexDescriptor = forSchema( forLabel( 1, 1 ) ).withId( indexId );
    private static final CapableIndexDescriptor capableIndexDescriptor = new CapableIndexDescriptor( storeIndexDescriptor, NO_CAPABILITY );
    private static final IndexProviderDescriptor providerDescriptor = IndexProviderDescriptor.UNDECIDED;
    private static final DirectoryFactory.InMemoryDirectoryFactory luceneDirectoryFactory = new DirectoryFactory.InMemoryDirectoryFactory();
    private static final Config config = Config.defaults();
    private static final IndexSamplingConfig samplingConfig = new IndexSamplingConfig( config );
    private static final RuntimeException sampleException = new RuntimeException( "Killroy messed with your index sample." );
    private IndexDirectoryStructure.Factory directoryFactory;

    @Before
    public void setup()
    {
        directoryFactory = defaultDirectoryStructure( dir.storeDir() );
    }

    /**
     * This test come from a support case where dropping an index would block forever after index sampling failed.
     * <p>
     * A fusion index has multiple {@link IndexSampler index samplers} that are called sequentially. If one fails, then the other will never be invoked.
     * This was a problem for {@link LuceneIndexSampler}. It owns a {@link org.neo4j.helpers.TaskControl} that it will try to release in try-finally
     * in {@link LuceneIndexSampler#sampleIndex()}. But it never gets here because a prior {@link IndexSampler} fails.
     * <p>
     * Because the {@link org.neo4j.helpers.TaskControl} was never released the lucene accessor would block forever, waiting for
     * {@link TaskCoordinator#awaitCompletion()}.
     * <p>
     * This situation was solved by making {@link IndexSampler} {@link java.io.Closeable} and include it in try-with-resource together with
     * {@link IndexReader} that created it.
     */
    @Test( timeout = 5_000L )
    public void failedIndexSamplingMustNotPreventIndexDrop() throws IOException, IndexEntryConflictException
    {
        LuceneIndexProvider luceneProvider = luceneProvider();
        makeSureIndexHasSomeData( luceneProvider ); // Otherwise no sampler will be created.

        IndexProvider failingProvider = failingProvider();
        FusionIndexProvider fusionProvider = createFusionProvider( luceneProvider, failingProvider );
        try ( IndexAccessor fusionAccessor = fusionProvider.getOnlineAccessor( storeIndexDescriptor, samplingConfig ) )
        {
            IndexSamplingJob indexSamplingJob = createIndexSamplingJob( fusionAccessor );

            // Call run from other thread
            try
            {
                indexSamplingJob.run();
            }
            catch ( RuntimeException e )
            {
                assertSame( e, sampleException );
            }

            // then
            fusionAccessor.drop();
            // should not block forever
        }
    }

    private void makeSureIndexHasSomeData( IndexProvider provider ) throws IOException, IndexEntryConflictException
    {
        try ( IndexAccessor accessor = provider.getOnlineAccessor(storeIndexDescriptor, samplingConfig );
              IndexUpdater updater = accessor.newUpdater( IndexUpdateMode.ONLINE ) )
        {
            updater.process( IndexEntryUpdate.add( 1, storeIndexDescriptor, Values.of( "some string" ) ) );
        }
    }

    private FusionIndexProvider createFusionProvider( LuceneIndexProvider luceneProvider, IndexProvider failingProvider )
    {
        SlotSelector slotSelector = SlotSelector.nullInstance;
        return new FusionIndexProvider( failingProvider, EMPTY, EMPTY, EMPTY, luceneProvider, slotSelector, providerDescriptor, directoryFactory, fs.get(),
                false );
    }

    private IndexSamplingJob createIndexSamplingJob( IndexAccessor fusionAccessor )
    {
        IndexStoreView storeView = IndexStoreView.EMPTY;
        IndexProxyAdapter indexProxy = new IndexProxyAdapter()
        {
            @Override
            public CapableIndexDescriptor getDescriptor()
            {
                return capableIndexDescriptor;
            }

            @Override
            public IndexReader newReader()
            {
                return fusionAccessor.newReader();
            }
        };
        OnlineIndexSamplingJobFactory onlineIndexSamplingJobFactory =
                new OnlineIndexSamplingJobFactory( storeView, simpleNameLookup, getInstance() );
        return onlineIndexSamplingJobFactory.create( 1, indexProxy );
    }

    private LuceneIndexProvider luceneProvider()
    {
        return new LuceneIndexProvider( fs.get(), luceneDirectoryFactory, directoryFactory, IndexProvider.Monitor.EMPTY, config, OperationalMode.single );
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
            public IndexAccessor getOnlineAccessor( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
            {
                return failingIndexAccessor();
            }
        };
    }

    private IndexAccessor failingIndexAccessor()
    {
        return new IndexAccessor.Adapter()
        {
            @Override
            public IndexReader newReader()
            {
                return new IndexReader.Adaptor()
                {
                    @Override
                    public IndexSampler createSampler()
                    {
                        return () -> {
                            throw sampleException;
                        };
                    }
                };
            }
        };
    }
}
