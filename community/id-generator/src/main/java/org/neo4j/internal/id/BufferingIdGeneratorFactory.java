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
package org.neo4j.internal.id;

import org.eclipse.collections.api.set.ImmutableSet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.collection.trackable.HeapTrackingLongArrayList;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

import static org.neo4j.internal.id.DiskBufferedIds.DEFAULT_SEGMENT_SIZE;
import static org.neo4j.internal.id.IdUtils.idFromCombinedId;
import static org.neo4j.internal.id.IdUtils.numberOfIdsFromCombinedId;

/**
 * Wraps {@link IdGenerator} so that ids can be freed using reuse marker at safe points in time, after all transactions
 * which were active at the time of freeing, have been closed.
 */
public class BufferingIdGeneratorFactory extends LifecycleAdapter implements IdGeneratorFactory
{
    public static final String PAGED_ID_BUFFER_FILE_NAME = "id-buffer.tmp";
    public static final Predicate<String> PAGED_ID_BUFFER_FILE_NAME_FILTER = new Predicate<>()
    {
        private final Pattern pattern = Pattern.compile( ".*" + PAGED_ID_BUFFER_FILE_NAME + ".+\\d$" );

        @Override
        public boolean test( String fileName )
        {
            return pattern.matcher( fileName ).matches();
        }
    };

    private final Map<IdType, BufferingIdGenerator> overriddenIdGenerators = new ConcurrentHashMap<>();
    private FileSystemAbstraction fs;
    private Path bufferBasePath;
    private Config config;
    private Supplier<IdController.TransactionSnapshot> snapshotSupplier;
    private IdController.IdFreeCondition condition;
    private MemoryTracker memoryTracker;
    private final IdGeneratorFactory delegate;
    private BufferedIds bufferQueue;
    private final IdTypeMapping idTypeMapping = new IdTypeMapping();
    private final Lock bufferWriteLock = new ReentrantLock();
    private final Lock bufferReadLock = new ReentrantLock();

    public BufferingIdGeneratorFactory( IdGeneratorFactory delegate )
    {
        this.delegate = delegate;
    }

    public void initialize( FileSystemAbstraction fs, Path bufferBasePath, Config config, Supplier<IdController.TransactionSnapshot> snapshotSupplier,
            IdController.IdFreeCondition condition, MemoryTracker memoryTracker ) throws IOException
    {
        this.fs = fs;
        this.bufferBasePath = bufferBasePath;
        this.config = config;
        this.snapshotSupplier = snapshotSupplier;
        this.condition = condition;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public IdGenerator open( PageCache pageCache, Path filename, IdType idType, LongSupplier highIdScanner, long maxId, DatabaseReadOnlyChecker readOnlyChecker,
            Config config, CursorContextFactory contextFactory, ImmutableSet<OpenOption> openOptions, IdSlotDistribution slotDistribution ) throws IOException
    {
        assert snapshotSupplier != null : "Factory needs to be initialized before usage";

        IdGenerator generator =
                delegate.open( pageCache, filename, idType, highIdScanner, maxId, readOnlyChecker, config, contextFactory, openOptions, slotDistribution );
        return wrapAndKeep( idType, generator );
    }

    @Override
    public IdGenerator create( PageCache pageCache, Path filename, IdType idType, long highId, boolean throwIfFileExists, long maxId,
            DatabaseReadOnlyChecker readOnlyChecker, Config config, CursorContextFactory contextFactory, ImmutableSet<OpenOption> openOptions,
            IdSlotDistribution slotDistribution ) throws IOException
    {
        IdGenerator idGenerator =
                delegate.create( pageCache, filename, idType, highId, throwIfFileExists, maxId, readOnlyChecker, config, contextFactory, openOptions,
                        slotDistribution );
        return wrapAndKeep( idType, idGenerator );
    }

    @Override
    public IdGenerator get( IdType idType )
    {
        IdGenerator generator = overriddenIdGenerators.get( idType );
        return generator != null ? generator : delegate.get( idType );
    }

    @Override
    public void visit( Consumer<IdGenerator> visitor )
    {
        overriddenIdGenerators.values().forEach( visitor );
    }

    @Override
    public void clearCache( CursorContext cursorContext )
    {
        delegate.clearCache( cursorContext );
    }

    @Override
    public Collection<Path> listIdFiles()
    {
        return delegate.listIdFiles();
    }

    private IdGenerator wrapAndKeep( IdType idType, IdGenerator generator )
    {
        int id = idTypeMapping.map( idType );
        BufferingIdGenerator bufferingGenerator = new BufferingIdGenerator( generator, id, memoryTracker, () -> collectAndOffloadBufferedIds( false ) );
        overriddenIdGenerators.put( idType, bufferingGenerator );
        return bufferingGenerator;
    }

    public void maintenance( CursorContext cursorContext )
    {
        collectAndOffloadBufferedIds( true );

        // Check and free deleted IDs that are safe to free
        bufferReadLock.lock();
        try
        {
            bufferQueue.read( new IdFreer( cursorContext ) );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            bufferReadLock.unlock();
        }
        overriddenIdGenerators.values().forEach( generator -> generator.maintenance( cursorContext ) );
    }

    private void collectAndOffloadBufferedIds( boolean blocking )
    {
        if ( blocking )
        {
            bufferWriteLock.lock();
        }
        else if ( !bufferWriteLock.tryLock() )
        {
            return;
        }

        try
        {
            List<IdBuffer> buffers = new ArrayList<>();
            overriddenIdGenerators.values().forEach( generator -> generator.collectBufferedIds( buffers ) );
            if ( !buffers.isEmpty() )
            {
                bufferQueue.write( snapshotSupplier.get(), buffers );
            }
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
        finally
        {
            bufferWriteLock.unlock();
        }
    }

    @Override
    public void init() throws Exception
    {
        this.bufferQueue = config.get( GraphDatabaseInternalSettings.buffered_ids_offload )
                ? new DiskBufferedIds( fs, bufferBasePath, memoryTracker, DEFAULT_SEGMENT_SIZE )
                : new HeapBufferedIds();
    }

    @Override
    public void shutdown() throws Exception
    {
        bufferReadLock.lock();
        bufferWriteLock.lock();
        try
        {
            IOUtils.closeAllUnchecked( bufferQueue );
            overriddenIdGenerators.clear();
            idTypeMapping.clear();
        }
        finally
        {
            bufferWriteLock.unlock();
            bufferReadLock.unlock();
        }
    }

    record IdBuffer(int idTypeOrdinal, HeapTrackingLongArrayList ids) implements AutoCloseable
    {
        @Override
        public void close()
        {
            ids.close();
        }
    }

    private class IdFreer implements BufferedIds.BufferedIdVisitor
    {
        private final CursorContext cursorContext;
        private IdGenerator.Marker marker;

        IdFreer( CursorContext cursorContext )
        {
            this.cursorContext = cursorContext;
        }

        @Override
        public boolean startChunk( IdController.TransactionSnapshot snapshot )
        {
            return condition.eligibleForFreeing( snapshot );
        }

        @Override
        public void startType( int idTypeOrdinal )
        {
            marker = overriddenIdGenerators.get( idTypeMapping.get( idTypeOrdinal ) ).delegate.marker( cursorContext );
        }

        @Override
        public void id( long id )
        {
            marker.markFree( idFromCombinedId( id ), numberOfIdsFromCombinedId( id ) );
        }

        @Override
        public void endType()
        {
            marker.close();
        }

        @Override
        public void endChunk()
        {
        }
    }

    private static class IdTypeMapping
    {
        private final List<IdType> idTypes = new CopyOnWriteArrayList<>();

        int map( IdType idType )
        {
            Preconditions.checkState( !idTypes.contains( idType ), "IdType %s already added", idType );
            idTypes.add( idType );
            return idTypes.size() - 1;
        }

        IdType get( int value )
        {
            return idTypes.get( value );
        }

        void clear()
        {
            idTypes.clear();
        }
    }
}
