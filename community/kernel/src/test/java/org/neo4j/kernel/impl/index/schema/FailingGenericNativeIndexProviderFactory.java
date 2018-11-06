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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.util.Collection;
import java.util.EnumSet;

import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.storageengine.api.schema.IndexSample;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;

import static java.util.Arrays.copyOfRange;

/**
 * Testing utility which takes a fully functional {@link GenericNativeIndexProviderFactory} and turns it into a provider which
 * is guaranteed to fail for various reasons, e.g. failing index population with the goal of creating an index which is in a
 * {@link InternalIndexState#FAILED} state. To get to this state in high-level testing is surprisingly hard,
 * so this test utility helps a lot to accomplish this.
 *
 * To be sure to use this provider in your test please do something like:
 * <pre>
 * db = new TestGraphDatabaseFactory()
 *     .removeKernelExtensions( TestGraphDatabaseFactory.INDEX_PROVIDERS_FILTER )
 *     .addKernelExtension( new FailingGenericNativeIndexProviderFactory( FailureType.INITIAL_STATE ) )
 *     .newEmbeddedDatabase( dir );
 * </pre>
 */
public class FailingGenericNativeIndexProviderFactory extends KernelExtensionFactory<GenericNativeIndexProviderFactory.Dependencies>
{
    public static final String INITIAL_STATE_FAILURE_MESSAGE = "Override initial state as failed";
    public static final String POPULATION_FAILURE_MESSAGE = "Fail on update during population";

    public enum FailureType
    {
        POPULATION,
        INITIAL_STATE
    }

    private final GenericNativeIndexProviderFactory actual;
    private final EnumSet<FailureType> failureTypes;

    public FailingGenericNativeIndexProviderFactory( FailureType... failureTypes )
    {
        this( new GenericNativeIndexProviderFactory(), 10_000, failureTypes );
    }

    private FailingGenericNativeIndexProviderFactory( GenericNativeIndexProviderFactory actual, int priority, FailureType... failureTypes )
    {
        super( ExtensionType.DATABASE, actual.getKeys().iterator().next() );
        if ( failureTypes.length == 0 )
        {
            throw new IllegalArgumentException( "At least one failure type, otherwise there's no point in this provider" );
        }
        this.actual = actual;
        this.failureTypes = EnumSet.of( failureTypes[0], copyOfRange( failureTypes, 1, failureTypes.length ) );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, GenericNativeIndexProviderFactory.Dependencies dependencies )
    {
        IndexProvider actualProvider = actual.newInstance( context, dependencies );
        return new IndexProvider( actualProvider.getProviderDescriptor(), IndexDirectoryStructure.given( actualProvider.directoryStructure() ) )
        {
            @Override
            public IndexPopulator getPopulator( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig )
            {
                IndexPopulator actualPopulator = actualProvider.getPopulator( descriptor, samplingConfig );
                if ( failureTypes.contains( FailureType.POPULATION ) )
                {
                    return new IndexPopulator()
                    {
                        @Override
                        public void create()
                        {
                            actualPopulator.create();
                        }

                        @Override
                        public void drop()
                        {
                            actualPopulator.drop();
                        }

                        @Override
                        public void add( Collection<? extends IndexEntryUpdate<?>> updates )
                        {
                            throw new RuntimeException( POPULATION_FAILURE_MESSAGE );
                        }

                        @Override
                        public void verifyDeferredConstraints( NodePropertyAccessor nodePropertyAccessor ) throws IndexEntryConflictException
                        {
                            actualPopulator.verifyDeferredConstraints( nodePropertyAccessor );
                        }

                        @Override
                        public IndexUpdater newPopulatingUpdater( NodePropertyAccessor accessor )
                        {
                            return actualPopulator.newPopulatingUpdater( accessor );
                        }

                        @Override
                        public void close( boolean populationCompletedSuccessfully )
                        {
                            actualPopulator.close( populationCompletedSuccessfully );
                        }

                        @Override
                        public void markAsFailed( String failure )
                        {
                            actualPopulator.markAsFailed( failure );
                        }

                        @Override
                        public void includeSample( IndexEntryUpdate<?> update )
                        {
                            actualPopulator.includeSample( update );
                        }

                        @Override
                        public IndexSample sampleResult()
                        {
                            return actualPopulator.sampleResult();
                        }
                    };
                }
                return actualPopulator;
            }

            @Override
            public IndexAccessor getOnlineAccessor( StoreIndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
            {
                return actualProvider.getOnlineAccessor( descriptor, samplingConfig );
            }

            @Override
            public String getPopulationFailure( StoreIndexDescriptor descriptor ) throws IllegalStateException
            {
                return failureTypes.contains( FailureType.INITIAL_STATE ) ? INITIAL_STATE_FAILURE_MESSAGE : actualProvider.getPopulationFailure( descriptor );
            }

            @Override
            public InternalIndexState getInitialState( StoreIndexDescriptor descriptor )
            {
                return failureTypes.contains( FailureType.INITIAL_STATE ) ? InternalIndexState.FAILED : actualProvider.getInitialState( descriptor );
            }

            @Override
            public IndexCapability getCapability( StoreIndexDescriptor descriptor )
            {
                return actualProvider.getCapability( descriptor );
            }

            @Override
            public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
            {
                return actualProvider.storeMigrationParticipant( fs, pageCache );
            }
        };
    }
}
