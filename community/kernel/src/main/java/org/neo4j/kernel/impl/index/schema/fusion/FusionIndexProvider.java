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
package org.neo4j.kernel.impl.index.schema.fusion;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
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
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueCategory;

import static org.neo4j.internal.kernel.api.InternalIndexState.FAILED;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.INSTANCE_COUNT;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.LUCENE;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.NUMBER;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.SPATIAL;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.STRING;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.TEMPORAL;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.forAll;
import static org.neo4j.kernel.impl.index.schema.fusion.FusionIndexBase.instancesAs;

/**
 * This {@link IndexProvider index provider} act as one logical index but is backed by four physical
 * indexes, the number, spatial, temporal native indexes, and the general purpose lucene index.
 */
public class FusionIndexProvider extends IndexProvider
{
    interface Selector
    {
        void validateSatisfied( Object[] instances );

        int selectSlot( Value... values );

        default <T> T select( T[] instances, Value... values )
        {
            return instances[selectSlot( values )];
        }

        /**
         * @return Appropriate IndexReader for given predicate or null if predicate needs all readers.
         */
        IndexReader select( IndexReader[] instances, IndexQuery... predicates );
    }

    private final boolean archiveFailedIndex;
    private final IndexProvider[] providers = new IndexProvider[INSTANCE_COUNT];
    private final Selector selector;
    private final DropAction dropAction;

    public FusionIndexProvider(
            // good to be strict with specific providers here since this is dev facing
            IndexProvider stringProvider,
            IndexProvider numberProvider,
            IndexProvider spatialProvider,
            IndexProvider temporalProvider,
            IndexProvider luceneProvider,
            Selector selector,
            Descriptor descriptor,
            int priority,
            IndexDirectoryStructure.Factory directoryStructure,
            FileSystemAbstraction fs,
            boolean archiveFailedIndex )
    {
        super( descriptor, priority, directoryStructure );
        fillProvidersArray( stringProvider, numberProvider, spatialProvider, temporalProvider, luceneProvider );
        selector.validateSatisfied( providers );
        this.archiveFailedIndex = archiveFailedIndex;
        this.selector = selector;
        this.dropAction = new FileSystemDropAction( fs, directoryStructure() );
    }

    private void fillProvidersArray( IndexProvider stringProvider, IndexProvider numberProvider, IndexProvider spatialProvider, IndexProvider temporalProvider,
            IndexProvider luceneProvider )
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
        return new FusionIndexPopulator( instancesAs( providers, IndexPopulator.class,
                provider -> provider.getPopulator( indexId, descriptor, samplingConfig ) ), selector, indexId, dropAction, archiveFailedIndex );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, SchemaIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig ) throws IOException
    {
        return new FusionIndexAccessor(
                instancesAs( providers, IndexAccessor.class, provider -> provider.getOnlineAccessor( indexId, descriptor, samplingConfig ) ),
                selector, indexId, descriptor, dropAction );
    }

    @Override
    public String getPopulationFailure( long indexId, SchemaIndexDescriptor descriptor ) throws IllegalStateException
    {
        StringBuilder builder = new StringBuilder();
        forAll( p -> writeFailure( p.getClass().getSimpleName(), builder, p, indexId, descriptor ), providers );
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
        InternalIndexState[] states = FusionIndexBase.instancesAs( providers, InternalIndexState.class, p -> p.getInitialState( indexId, descriptor ) );
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
        IndexCapability[] capabilities = new IndexCapability[providers.length];
        for ( int i = 0; i < providers.length; i++ )
        {
            capabilities[i] = providers[i].getCapability( schemaIndexDescriptor );
        }
        return new UnionIndexCapability( capabilities )
        {
            @Override
            public IndexOrder[] orderCapability( ValueCategory... valueCategories )
            {
                // No order capability when combining results from different indexes
                if ( valueCategories.length == 1 && valueCategories[0] == ValueCategory.UNKNOWN )
                {
                    return new IndexOrder[0];
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
