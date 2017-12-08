/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema.spatial;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.newapi.UnionIndexCapability;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;

import static java.util.stream.Collectors.joining;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

/**
 * This {@link SchemaIndexProvider index provider} act as one logical index but is backed by several physical indexes.
 */
public class SpatialFusionSchemaIndexProvider extends SchemaIndexProvider
{
    private static final String KEY = "spatial";
    private static final Descriptor SPATIAL_PROVIDER_DESCRIPTOR = new Descriptor( KEY, "1.0" );
    private static final int PRIORITY = 2;

    public interface Selector
    {
        <T> T select( Map<CoordinateReferenceSystem, T> instances, Value... values );
    }

    private Map<CoordinateReferenceSystem,SchemaIndexProvider> providerMap = new HashMap<>();
    private final Selector selector;
    private final DropAction dropAction;

    public SpatialFusionSchemaIndexProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory directoryStructure, Monitor monitor,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, boolean readOnly )
    {
        super( SPATIAL_PROVIDER_DESCRIPTOR, PRIORITY, directoryStructure );
        this.dropAction = new FileSystemDropAction( fs, directoryStructure() );
        this.selector = new SpatialSelector();
        IndexDirectoryStructure.Factory childDirectoryStructure = directoriesBySubProvider(directoryStructure.forProvider( SPATIAL_PROVIDER_DESCRIPTOR ));

        // TODO need to create all providers here?
        providerMap.put( CoordinateReferenceSystem.Cartesian,
                new SpatialSchemaIndexProvider( pageCache, fs, childDirectoryStructure, monitor, recoveryCleanupWorkCollector, readOnly ) );
    }

    @Override
    public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
    {
        Map<CoordinateReferenceSystem,IndexPopulator> populatorMap = new HashMap<>();
        for ( Map.Entry<CoordinateReferenceSystem, SchemaIndexProvider> provider : providerMap.entrySet() )
        {
            populatorMap.put( provider.getKey(), provider.getValue().getPopulator( indexId, descriptor, samplingConfig ) );
        }
        return new SpatialFusionIndexPopulator( populatorMap, selector, indexId, dropAction );
    }

    @Override
    public IndexAccessor getOnlineAccessor( long indexId, IndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
    {
        Map<CoordinateReferenceSystem,IndexAccessor> accessorMap = new HashMap<>();
        for ( Map.Entry<CoordinateReferenceSystem, SchemaIndexProvider> provider : providerMap.entrySet() )
        {
            accessorMap.put( provider.getKey(), provider.getValue().getOnlineAccessor( indexId, descriptor, samplingConfig ) );
        }
        return new SpatialFusionIndexAccessor(accessorMap, selector, indexId, descriptor, dropAction );
    }

    @Override
    public String getPopulationFailure( long indexId ) throws IllegalStateException
    {
        Map<CoordinateReferenceSystem,String> failureMap = new HashMap<>();
        for ( Map.Entry<CoordinateReferenceSystem, SchemaIndexProvider> provider : providerMap.entrySet() )
        {
            try
            {
                failureMap.put( provider.getKey(), provider.getValue().getPopulationFailure( indexId ) );
            }
            catch ( IllegalStateException e )
            {   // Just catch
            }
        }
        if (failureMap.isEmpty())
        {
            throw new IllegalStateException( "None of the indexes were in a failed state" );
        }

        return failureMap.entrySet().stream().map( e -> e.getKey() + ": " + e.getValue() ).collect( joining( " " ) );
    }

    @Override
    public InternalIndexState getInitialState( long indexId, IndexDescriptor descriptor )
    {
        Set<InternalIndexState> states = providerMap.entrySet().stream().map( e -> e.getValue().getInitialState( indexId, descriptor ) ).collect( Collectors.toSet());
        return states.stream().reduce( InternalIndexState.ONLINE, (acc, s) -> {
            if (acc == InternalIndexState.FAILED || s == InternalIndexState.FAILED )
            {
                // One of the state is FAILED, the whole state must be considered FAILED
                return InternalIndexState.FAILED;
            }
            if (acc == InternalIndexState.POPULATING || s == InternalIndexState.POPULATING )
            {
                // No state is FAILED and one of the state is POPULATING, the whole state must be considered POPULATING
                return InternalIndexState.POPULATING;
            }
            return acc;
        } );
    }

    @Override
    public IndexCapability getCapability( IndexDescriptor indexDescriptor )
    {
        IndexCapability[] capabilities = providerMap.entrySet().stream().map( e -> e.getValue().getCapability( indexDescriptor ) ).collect( Collectors.toList() ).toArray( new IndexCapability[providerMap.size()] );
        return new UnionIndexCapability( capabilities )
        {
            @Override
            public IndexOrder[] orderCapability( ValueGroup... valueGroups )
            {
                // No order capability when combining results from different indexes
                if ( valueGroups.length == 1 && valueGroups[0] == ValueGroup.UNKNOWN )
                {
                    return new IndexOrder[0];
                }
                // Otherwise union of capabilities
                return super.orderCapability( valueGroups );
            }
        };
    }

    @Override
    public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
    {
        // TODO implementation of this depends on decisions around defaults and migration. Coming soon.
        return StoreMigrationParticipant.NOT_PARTICIPATING;
    }

    static IndexSample combineSamples( IndexSample... samples )
    {
        long indexSize = Arrays.stream( samples ).mapToLong( IndexSample::indexSize ).sum();
        long uniqueValues = Arrays.stream( samples ).mapToLong( IndexSample::uniqueValues ).sum();
        long sampleSize = Arrays.stream( samples ).mapToLong( IndexSample::sampleSize ).sum();
        return new IndexSample( indexSize, uniqueValues, sampleSize );
    }

    /**
     * As an interface because this is actually dependent on whether or not an index lives on a {@link FileSystemAbstraction}
     * or a page cache. At the time of writing this there's only the possibility to put these on the file system,
     * but there will be a possibility to put these in the page cache file management instead and having this abstracted
     * will help when making that switch/decision.
     */
    @FunctionalInterface
    interface DropAction
    {
        /**
         * Deletes the index directory and everything in it, as last part of dropping an index.
         *
         * @param indexId the index id, for which directory to drop.
         * @throws IOException on I/O error.
         */
        void drop( long indexId ) throws IOException;
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
        public void drop( long indexId ) throws IOException
        {
            fs.deleteRecursively( directoryStructure.directoryForIndex( indexId ) );
        }
    }
}
