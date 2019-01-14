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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
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
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.newapi.UnionIndexCapability;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.values.storable.ValueCategory;

import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.INSTANCE_COUNT;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.SlotSelector.TEMPORAL;

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
        IndexProvider[] providers = new IndexProvider[INSTANCE_COUNT];
        fillProvidersArray( providers, stringProvider, numberProvider, spatialProvider, temporalProvider, luceneProvider );
        slotSelector.validateSatisfied( providers );
        this.archiveFailedIndex = archiveFailedIndex;
        this.slotSelector = slotSelector;
        this.providers = new InstanceSelector<>( providers );
        this.dropAction = new FileSystemDropAction( fs, directoryStructure() );
    }

    private void fillProvidersArray( IndexProvider[] providers,
            IndexProvider stringProvider, IndexProvider numberProvider, IndexProvider spatialProvider,
            IndexProvider temporalProvider, IndexProvider luceneProvider )
    {
        providers[STRING] = stringProvider;
        providers[NUMBER] = numberProvider;
        providers[SPATIAL] = spatialProvider;
        providers[TEMPORAL] = temporalProvider;
        providers[LUCENE] = luceneProvider;
    }

    @Override
    public IndexPopulator getPopulator( long indexId, SchemaIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        IndexPopulator[] populators =
                providers.instancesAs( new IndexPopulator[INSTANCE_COUNT], provider -> provider.getPopulator( indexId, descriptor, samplingConfig ) );
        return new FusionIndexPopulator( slotSelector, new InstanceSelector<>( populators ), indexId, dropAction, archiveFailedIndex );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        IndexAccessor[] accessors =
                providers.instancesAs( new IndexAccessor[INSTANCE_COUNT], provider -> provider.getOnlineAccessor( indexId, descriptor, samplingConfig ) );
        return new FusionIndexAccessor( slotSelector, new InstanceSelector<>( accessors ), indexId, descriptor, dropAction );
    }

    @Override
    public String getPopulationFailure( long indexId, SchemaIndexDescriptor descriptor ) throws IllegalStateException
    {
        StringBuilder builder = new StringBuilder();
        providers.forAll( p -> writeFailure( p.getClass().getSimpleName(), builder, p, indexId, descriptor ) );
        String failure = builder.toString();
        if ( !failure.isEmpty() )
        {
            return failure;
        }
        throw new IllegalStateException( "None of the indexes were in a failed state" );
    }

    private void writeFailure( String indexName, StringBuilder builder, IndexProvider provider, long indexId, SchemaIndexDescriptor descriptor )
    {
        try
        {
            String failure = provider.getPopulationFailure( indexId, descriptor );
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
    public InternalIndexState getInitialState( long indexId, SchemaIndexDescriptor descriptor )
    {
        InternalIndexState[] states = providers.instancesAs( new InternalIndexState[INSTANCE_COUNT], p -> p.getInitialState( indexId, descriptor ) );
        if ( Arrays.stream( states ).anyMatch( state -> state == FAILED ) )
        {
            // One of the state is FAILED, the whole state must be considered FAILED
            return FAILED;
        }
        if ( Arrays.stream( states ).anyMatch( state -> state == POPULATING ) )
        {
            // No state is FAILED and one of the state is POPULATING, the whole state must be considered POPULATING
            return POPULATING;
        }
        // This means that all parts are ONLINE
        return InternalIndexState.ONLINE;
    }

    @Override
    public IndexCapability getCapability( SchemaIndexDescriptor schemaIndexDescriptor )
    {
        IndexCapability[] capabilities =
                providers.instancesAs( new IndexCapability[INSTANCE_COUNT], provider -> provider.getCapability( schemaIndexDescriptor ) );
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
         * @throws IOException on I/O error.
         * @see GraphDatabaseSettings#archive_failed_index
         */
        void drop( long indexId, boolean archiveExistentIndex ) throws IOException;

        /**
         * Deletes the index directory and everything in it, as last part of dropping an index.
         *
         * @param indexId the index id, for which directory to drop.
         * @throws IOException on I/O error.
         */
        default void drop( long indexId ) throws IOException
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
        public void drop( long indexId, boolean archiveExistentIndex ) throws IOException
        {
            File rootIndexDirectory = directoryStructure.directoryForIndex( indexId );
            if ( archiveExistentIndex && !FileUtils.isEmptyDirectory( rootIndexDirectory ) )
            {
                ZipUtils.zip( fs, rootIndexDirectory, archiveFile( rootIndexDirectory ) );
            }
            fs.deleteRecursively( rootIndexDirectory );
        }

        private File archiveFile( File folder )
        {
            return new File( folder.getParent(), "archive-" + folder.getName() + "-" + System.currentTimeMillis() + ".zip" );
        }
    }
}
