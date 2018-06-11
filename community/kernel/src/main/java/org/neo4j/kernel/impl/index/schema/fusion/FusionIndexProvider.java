/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.EnumMap;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.compress.ZipUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.StoreIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.newapi.UnionIndexCapability;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.values.storable.ValueCategory;

import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.IndexSlot.TEMPORAL;

/**
 * This {@link IndexProvider index provider} act as one logical index but is backed by four physical
 * indexes, the number, spatial, temporal native indexes, and the general purpose lucene index.
 */
public class FusionIndexProvider extends IndexProvider
{
    private final boolean archiveFailedIndex;
    private final InstanceSelector<IndexProvider> providers;
    private final SlotSelector slotSelector;
    private final DropAction dropAction;

    public FusionIndexProvider(
            // good to be strict with specific providers here since this is dev facing
            IndexProvider stringProvider,
            IndexProvider numberProvider,
            IndexProvider spatialProvider,
            IndexProvider temporalProvider,
            IndexProvider luceneProvider,
            SlotSelector slotSelector,
            Descriptor descriptor,
            int priority,
            IndexDirectoryStructure.Factory directoryStructure,
            FileSystemAbstraction fs,
            boolean archiveFailedIndex )
    {
        super( descriptor, priority, directoryStructure );
        this.archiveFailedIndex = archiveFailedIndex;
        this.slotSelector = slotSelector;
        this.providers = new InstanceSelector<>();
        this.dropAction = new FileSystemDropAction( fs, directoryStructure() );
        fillProvidersSelector( stringProvider, numberProvider, spatialProvider, temporalProvider, luceneProvider );
        slotSelector.validateSatisfied( providers );
    }

    private void fillProvidersSelector(
            IndexProvider stringProvider, IndexProvider numberProvider, IndexProvider spatialProvider,
            IndexProvider temporalProvider, IndexProvider luceneProvider )
    {
        providers.put( STRING, stringProvider );
        providers.put( NUMBER, numberProvider );
        providers.put( SPATIAL, spatialProvider );
        providers.put( TEMPORAL, temporalProvider );
        providers.put( LUCENE, luceneProvider );
    }

    @Override
    public IndexPopulator getPopulator( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        EnumMap<IndexSlot,IndexPopulator> populators = providers.map( provider -> provider.getPopulator( descriptor, samplingConfig ) );
        return new FusionIndexPopulator( slotSelector, new InstanceSelector<>( populators ), descriptor.getId(), dropAction, archiveFailedIndex );
    }

    @Override
    public IndexAccessor getOnlineAccessor( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        EnumMap<IndexSlot,IndexAccessor> accessors = providers.map( provider -> provider.getOnlineAccessor( descriptor, samplingConfig ) );
        return new FusionIndexAccessor( slotSelector, new InstanceSelector<>( accessors ), descriptor, dropAction );
    }

    @Override
    public String getPopulationFailure( StoreIndexDescriptor descriptor ) throws IllegalStateException
    {
        StringBuilder builder = new StringBuilder();
        providers.forAll( p -> writeFailure( p.getClass().getSimpleName(), builder, p, descriptor ) );
        String failure = builder.toString();
        if ( !failure.isEmpty() )
        {
            return failure;
        }
        throw new IllegalStateException( "None of the indexes were in a failed state" );
    }

    private void writeFailure( String indexName, StringBuilder builder, IndexProvider provider, StoreIndexDescriptor descriptor )
    {
        try
        {
            String failure = provider.getPopulationFailure( descriptor );
            builder.append( indexName );
            builder.append( ": " );
            builder.append( failure );
            builder.append( ' ' );
        }
        catch ( IllegalStateException e )
        {   // Just catch
        }
    }

    @Override
    public InternalIndexState getInitialState( StoreIndexDescriptor descriptor )
    {
        Iterable<InternalIndexState> statesIterable = providers.transform( p -> p.getInitialState( descriptor ) );
        List<InternalIndexState> states = Iterables.asList( statesIterable );
        if ( states.contains( FAILED ) )
        {
            // One of the state is FAILED, the whole state must be considered FAILED
            return FAILED;
        }
        if ( states.contains( POPULATING ) )
        {
            // No state is FAILED and one of the state is POPULATING, the whole state must be considered POPULATING
            return POPULATING;
        }
        // This means that all parts are ONLINE
        return InternalIndexState.ONLINE;
    }

    @Override
    public IndexCapability getCapability()
    {
        Iterable<IndexCapability> capabilities = providers.transform( IndexProvider::getCapability );
        return new UnionIndexCapability( capabilities )
        {
            @Override
            public IndexOrder[] orderCapability( ValueCategory... valueCategories )
            {
                // No order capability when combining results from different indexes
                if ( valueCategories.length == 1 && valueCategories[0] == ValueCategory.UNKNOWN )
                {
                    return ORDER_NONE;
                }
                // Otherwise union of capabilities
                return super.orderCapability( valueCategories );
            }
        };
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    @FunctionalInterface
    interface DropAction
    {
        /**
         * Deletes the index directory and everything in it, as last part of dropping an index.
         * Can be configured to create archive with content of index directories for future analysis.
         *
         * @param indexId the index id, for which directory to drop.
         * @param archiveExistentIndex create archive with content of dropped directories
         * @see GraphDatabaseSettings#archive_failed_index
         */
        void drop( long indexId, boolean archiveExistentIndex );

        /**
         * Deletes the index directory and everything in it, as last part of dropping an index.
         *
         * @param indexId the index id, for which directory to drop.
         */
        default void drop( long indexId )
        {
            drop( indexId, false );
        }
    }

    private static class FileSystemDropAction implements DropAction
    {
        private final FileSystemAbstraction fs;
        private final IndexDirectoryStructure directoryStructure;

        FileSystemDropAction( FileSystemAbstraction fs, IndexDirectoryStructure directoryStructure )
        {
            this.fs = fs;
            this.directoryStructure = directoryStructure;
        }

        @Override
        public void drop( long indexId, boolean archiveExistentIndex )
        {
            try
            {
                File rootIndexDirectory = directoryStructure.directoryForIndex( indexId );
                if ( archiveExistentIndex && !FileUtils.isEmptyDirectory( rootIndexDirectory ) )
                {
                    ZipUtils.zip( fs, rootIndexDirectory, archiveFile( rootIndexDirectory ) );
                }
                fs.deleteRecursively( rootIndexDirectory );
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        }

        private File archiveFile( File folder )
        {
            return new File( folder.getParent(), "archive-" + folder.getName() + "-" + System.currentTimeMillis() + ".zip" );
        }
    }
}
