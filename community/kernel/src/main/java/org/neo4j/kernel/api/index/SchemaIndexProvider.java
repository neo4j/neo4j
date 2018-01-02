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
package org.neo4j.kernel.api.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.graphdb.DependencyResolver.SelectionStrategy;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.storemigration.StoreMigrationParticipant;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.helpers.collection.IteratorUtil.addToCollection;
import static org.neo4j.kernel.extension.KernelExtensionUtil.servicesClassPathEntryInformation;

/**
 * Contract for implementing an index in Neo4j.
 *
 * This is a sensitive thing to implement, because it manages data that is controlled by
 * Neo4js logical log. As such, the implementation needs to behave under some rather strict rules.
 *
 * <h3>Populating the index</h3>
 *
 * When an index rule is added, the {@link IndexingService} is notified. It will, in turn, ask
 * your {@link SchemaIndexProvider} for a\
 * {@link #getPopulator(long, IndexDescriptor, IndexConfiguration, IndexSamplingConfig) batch index writer}.
 *
 * A background index job is triggered, and all existing data that applies to the new rule, as well as new data
 * from the "outside", will be inserted using the writer. You are guaranteed that usage of this writer,
 * during population, will be single threaded.
 *
 * These are the rules you must adhere to here:
 *
 * <ul>
 * <li>You CANNOT say that the state of the index is {@link InternalIndexState#ONLINE}</li>
 * <li>You MUST store all updates given to you</li>
 * <li>You MAY persistently store the updates</li>
 * </ul>
 *
 *
 * <h3>The Flip</h3>
 *
 * Once population is done, the index needs to be "flipped" to an online mode of operation.
 *
 * The index will be notified, through the {@link org.neo4j.kernel.api.index.IndexPopulator#close(boolean)}
 * method, that population is done, and that the index should turn it's state to {@link InternalIndexState#ONLINE} or
 * {@link InternalIndexState#FAILED} depending on the value given to the
 * {@link org.neo4j.kernel.api.index.IndexPopulator#close(boolean) close method}.
 *
 * If the index is persisted to disk, this is a <i>vital</i> part of the index lifecycle.
 * For a persisted index, the index MUST NOT store the state as online unless it first guarantees that the entire index
 * is flushed to disk. Failure to do so could produce a situation where, after a crash,
 * an index is believed to be online when it in fact was not yet fully populated. This would break the database
 * recovery process.
 *
 * If you are implementing this interface, you can choose to not store index state. In that case,
 * you should report index state as {@link InternalIndexState#POPULATING} upon startup.
 * This will cause the database to re-create the index from scratch again.
 *
 * These are the rules you must adhere to here:
 *
 * <ul>
 * <li>You MUST have flushed the index to durable storage if you are to persist index state as {@link InternalIndexState#ONLINE}</li>
 * <li>You MAY decide not to store index state</li>
 * <li>If you don't store index state, you MUST default to {@link InternalIndexState#POPULATING}</li>
 * </ul>
 *
 * <h3>Online operation</h3>
 *
 * Once the index is online, the database will move to using the
 * {@link #getOnlineAccessor(long, IndexConfiguration, IndexSamplingConfig) online accessor} to
 * write to the index.
 */
public abstract class SchemaIndexProvider extends LifecycleAdapter implements Comparable<SchemaIndexProvider>
{
    public static final SchemaIndexProvider NO_INDEX_PROVIDER =
            new SchemaIndexProvider( new Descriptor( "no-index-provider", "1.0" ), -1 )
            {
                private final IndexAccessor singleWriter = new IndexAccessor.Adapter();
                private final IndexPopulator singlePopulator = new IndexPopulator.Adapter();

                @Override
                public IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config,
                                                        IndexSamplingConfig samplingConfig )
                {
                    return singleWriter;
                }

                @Override
                public IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexConfiguration config,
                                                    IndexSamplingConfig samplingConfig )
                {
                    return singlePopulator;
                }

                @Override
                public InternalIndexState getInitialState( long indexId )
                {
                    return InternalIndexState.POPULATING;
                }

                @Override
                public StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache )
                {
                    return StoreMigrationParticipant.NOT_PARTICIPATING;
                }

                @Override
                public String getPopulationFailure( long indexId ) throws IllegalStateException
                {
                    throw new IllegalStateException();
                }
            };

    public static final SelectionStrategy HIGHEST_PRIORITIZED_OR_NONE =
            new SelectionStrategy()
    {
        @Override
        @SuppressWarnings("unchecked")
        public <T> T select( Class<T> type, Iterable<T> candidates ) throws IllegalArgumentException
        {
            List<Comparable> all = (List<Comparable>) addToCollection( candidates, new ArrayList<T>() );
            if ( all.isEmpty() )
            {
                throw new IllegalArgumentException( "No schema index provider " +
                        SchemaIndexProvider.class.getName() + " found. " + servicesClassPathEntryInformation() );
            }
            Collections.sort( all );
            return (T) all.get( all.size()-1 );
        }
    };

    protected final int priority;
    private final Descriptor providerDescriptor;

    protected SchemaIndexProvider( Descriptor descriptor, int priority )
    {
        assert descriptor != null;
        this.priority = priority;
        this.providerDescriptor = descriptor;
    }

    /**
     * Used for initially populating a created index, using batch insertion.
     */
    public abstract IndexPopulator getPopulator( long indexId, IndexDescriptor descriptor, IndexConfiguration config,
                                                 IndexSamplingConfig samplingConfig );

    /**
     * Used for updating an index once initial population has completed.
     */
    public abstract IndexAccessor getOnlineAccessor( long indexId, IndexConfiguration config,
                                                     IndexSamplingConfig samplingConfig ) throws IOException;

    /**
     * Returns a failure previously gotten from {@link IndexPopulator#markAsFailed(String)}
     *
     * Implementations are expected to persist this failure and may elect to make use of
     * {@link org.neo4j.kernel.api.index.util.FailureStorage} for this purpose
     */
    public abstract String getPopulationFailure( long indexId ) throws IllegalStateException;

    /**
     * Called during startup to find out which state an index is in. If {@link InternalIndexState#FAILED}
     * is returned then a further call to {@link #getPopulationFailure(long)} is expected and should return
     * the failure accepted by any call to {@link IndexPopulator#markAsFailed(String)} call at the time
     * of failure.
     */
    public abstract InternalIndexState getInitialState( long indexId );

    /**
     * @return a description of this index provider
     */
    public Descriptor getProviderDescriptor()
    {
        return providerDescriptor;
    }

    @Override
    public int compareTo( SchemaIndexProvider o )
    {
        return this.priority - o.priority;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        SchemaIndexProvider other = (SchemaIndexProvider) o;

        return priority == other.priority &&
               providerDescriptor.equals( other.providerDescriptor );
    }

    @Override
    public int hashCode()
    {
        int result = priority;
        result = 31 * result + (providerDescriptor != null ? providerDescriptor.hashCode() : 0);
        return result;
    }

    public static File getRootDirectory( File storeDir, String key )
    {
        return new File( new File( new File( storeDir, "schema" ), "index" ), key );
    }

    public abstract StoreMigrationParticipant storeMigrationParticipant( FileSystemAbstraction fs, PageCache pageCache );

    /**
     * Provides a snapshot of meta files about this index provider, not the indexes themselves.
     * @return
     */
    public ResourceIterator<File> snapshotMetaFiles()
    {
        return IteratorUtil.emptyIterator();
    }

    public static class Descriptor
    {
        private final String key;
        private final String version;

        public Descriptor( String key, String version )
        {
            if (key == null)
            {
                throw new IllegalArgumentException( "null provider key prohibited" );
            }
            if (key.length() == 0)
            {
                throw new IllegalArgumentException( "empty provider key prohibited" );
            }
            if (version == null)
            {
                throw new IllegalArgumentException( "null provider version prohibited" );
            }

            this.key = key;
            this.version = version;
        }

        public String getKey()
        {
            return key;
        }

        public String getVersion()
        {
            return version;
        }

        @Override
        public int hashCode()
        {
            return ( 23 + key.hashCode() ) ^ version.hashCode();
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( obj != null && obj instanceof Descriptor )
            {
                Descriptor otherDescriptor = (Descriptor) obj;
                return key.equals( otherDescriptor.getKey() ) && version.equals( otherDescriptor.getVersion() );
            }
            return false;
        }

        @Override
        public String toString()
        {
            return "{key=" + key + ", version=" + version + "}";
        }
    }
}
