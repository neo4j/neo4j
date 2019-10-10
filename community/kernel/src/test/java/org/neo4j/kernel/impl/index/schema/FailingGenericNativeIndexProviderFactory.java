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

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.memory.ByteBufferFactory;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.api.index.IndexSamplingConfig;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.updater.DelegatingIndexUpdater;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.storageengine.api.IndexEntryUpdate;

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
 *     .removeExtensions( TestGraphDatabaseFactory.INDEX_PROVIDERS_FILTER )
 *     .addExtension( new FailingGenericNativeIndexProviderFactory( FailureType.INITIAL_STATE ) )
 *     .newEmbeddedDatabase( dir );
 * </pre>
 */
public class FailingGenericNativeIndexProviderFactory extends ExtensionFactory<AbstractIndexProviderFactory.Dependencies>
{
    public static final String INITIAL_STATE_FAILURE_MESSAGE = "Override initial state as failed";
    public static final String POPULATION_FAILURE_MESSAGE = "Fail on update during population";

    public enum FailureType
    {
        POPULATION,
        INITIAL_STATE,
        SKIP_ONLINE_UPDATES
    }

    private final GenericNativeIndexProviderFactory actual;
    private final EnumSet<FailureType> failureTypes;

    public FailingGenericNativeIndexProviderFactory( FailureType... failureTypes )
    {
        this( new GenericNativeIndexProviderFactory(), failureTypes );
    }

    private FailingGenericNativeIndexProviderFactory( GenericNativeIndexProviderFactory actual, FailureType... failureTypes )
    {
        super( ExtensionType.DATABASE, actual.getName() );
        if ( failureTypes.length == 0 )
        {
            throw new IllegalArgumentException( "At least one failure type, otherwise there's no point in this provider" );
        }
        this.actual = actual;
        this.failureTypes = EnumSet.of( failureTypes[0], copyOfRange( failureTypes, 1, failureTypes.length ) );
    }

    @Override
    public Lifecycle newInstance( ExtensionContext context, AbstractIndexProviderFactory.Dependencies dependencies )
    {
        IndexProvider actualProvider = actual.newInstance( context, dependencies );
        return new IndexProvider.Delegating( actualProvider )
        {
            @Override
            public IndexPopulator getPopulator( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig, ByteBufferFactory bufferFactory )
            {
                IndexPopulator actualPopulator = actualProvider.getPopulator( descriptor, samplingConfig, bufferFactory );
                if ( failureTypes.contains( FailureType.POPULATION ) )
                {
                    return new IndexPopulator.Delegating( actualPopulator )
                    {
                        @Override
                        public void add( Collection<? extends IndexEntryUpdate<?>> updates )
                        {
                            throw new RuntimeException( POPULATION_FAILURE_MESSAGE );
                        }
                    };
                }
                return actualPopulator;
            }

            @Override
            public IndexAccessor getOnlineAccessor( IndexDescriptor descriptor, IndexSamplingConfig samplingConfig ) throws IOException
            {
                IndexAccessor actualAccessor = actualProvider.getOnlineAccessor( descriptor, samplingConfig );
                return new IndexAccessor.Delegating( actualAccessor )
                {
                    @Override
                    public IndexUpdater newUpdater( IndexUpdateMode mode )
                    {
                        IndexUpdater actualUpdater = actualAccessor.newUpdater( mode );
                        return new DelegatingIndexUpdater( actualUpdater )
                        {
                            @Override
                            public void process( IndexEntryUpdate<?> update ) throws IndexEntryConflictException
                            {
                                if ( !failureTypes.contains( FailureType.SKIP_ONLINE_UPDATES ) )
                                {
                                    super.process( update );
                                }
                            }
                        };
                    }
                };
            }

            @Override
            public String getPopulationFailure( IndexDescriptor descriptor )
            {
                return failureTypes.contains( FailureType.INITIAL_STATE ) ? INITIAL_STATE_FAILURE_MESSAGE : actualProvider.getPopulationFailure( descriptor );
            }

            @Override
            public InternalIndexState getInitialState( IndexDescriptor descriptor )
            {
                return failureTypes.contains( FailureType.INITIAL_STATE ) ? InternalIndexState.FAILED : actualProvider.getInitialState( descriptor );
            }
        };
    }
}
