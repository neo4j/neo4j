/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.counts;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.locking.LockWrapper;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.register.Register;
import org.neo4j.register.Registers;

import static org.neo4j.kernel.impl.store.counts.CountsKey.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.indexSizeKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.store.counts.CountsKey.relationshipKey;

/**
 * {@link CountsTracker} maintains two files, the {@link #alphaFile} and the {@link #betaFile} that it rotates between.
 * {@link #updateLock} is used to ensure that no updates happen while we rotate from one file to another. Reads are
 * still ok though, they just read whatever the current state is. The state is assigned atomically at the end of
 * rotation.
 */
public class CountsTracker implements CountsVisitor.Visitable, AutoCloseable, CountsAccessor
{
    public static final String STORE_DESCRIPTOR = SortedKeyValueStore.class.getSimpleName();

    public static final String ALPHA = ".alpha", BETA = ".beta";
    private final File alphaFile, betaFile;
    private final ReadWriteLock updateLock = new ReentrantReadWriteLock( /*fair=*/true );
    private volatile CountsTrackerState state;

    public CountsTracker( FileSystemAbstraction fs, PageCache pageCache, File storeFileBase )
    {
        this.alphaFile = storeFile( storeFileBase, ALPHA );
        this.betaFile = storeFile( storeFileBase, BETA );
        this.state = new ConcurrentCountsTrackerState( openStore( fs, pageCache, this.alphaFile, this.betaFile ) );
    }

    private static CountsStore openStore( FileSystemAbstraction fs, PageCache pageCache, File alpha, File beta )
    {
        try
        {
            boolean hasAlpha = fs.fileExists( alpha ), hasBeta = fs.fileExists( beta );
            if ( hasAlpha && hasBeta )
            {
                CountsStore alphaStore = CountsStore.open( fs, pageCache, alpha );
                CountsStore betaStore = CountsStore.open( fs, pageCache, beta );

                if ( isAlphaStoreMoreRecent( alphaStore, betaStore ) )
                {
                    betaStore.close();
                    return alphaStore;
                }
                else
                {
                    alphaStore.close();
                    return betaStore;
                }
            }
            else if ( hasAlpha )
            {
                return CountsStore.open( fs, pageCache, alpha );
            }
            else if ( hasBeta )
            {
                return CountsStore.open( fs, pageCache, beta );
            }
            else
            {
                throw new IllegalStateException( "Storage file not found." );
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private static boolean isAlphaStoreMoreRecent( CountsStore alphaStore, CountsStore betaStore )
    {
        long alphaTxId = alphaStore.lastTxId(), betaTxId = betaStore.lastTxId();
        long alphaVersion = alphaStore.minorVersion(), betaVersion = betaStore.minorVersion();

        // TODO: Check with txIdProvider to infer if these numbers make any sense

        if ( alphaTxId == betaTxId )
        {
            if ( alphaVersion == betaVersion )
            {
                throw new IllegalStateException( "Found two storage files with same last tx id and minor version" );
            }
            return alphaVersion > betaVersion;
        }
        else
        {
            return alphaTxId > betaTxId;
        }
    }

    public static void createEmptyCountsStore( PageCache pageCache, File file, String version )
    {
        CountsStore.createEmpty( pageCache, storeFile( file, ALPHA ), version );
    }

    public boolean acceptTx( long txId )
    {
        return state.lastTxId() < txId;
    }

    @Override
    public long nodeCount( int labelId )
    {
        return state.nodeCount( nodeKey( labelId ) );
    }

    @Override
    public long incrementNodeCount( int labelId, long delta )
    {
        if ( delta == 0 )
        {
            return nodeCount( labelId );
        }
        else
        {
            try ( LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
            {
                CountsKey.NodeKey key = nodeKey( labelId );
                long value = state.incrementNodeCount( key, delta );
                assert value >= 0 : String.format( "incrementNodeCount(key=%s, delta=%d) -> value=%d", key, delta, value );
                return value;
            }
        }
    }

    @Override
    public long relationshipCount( int startLabelId, int typeId, int endLabelId )
    {
        return state.relationshipCount( relationshipKey( startLabelId, typeId, endLabelId ) );
    }

    @Override
    public long incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
        if ( delta == 0 )
        {
            return relationshipCount( startLabelId, typeId, endLabelId );
        }
        {
            try ( LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
            {
                CountsKey.RelationshipKey key = relationshipKey( startLabelId, typeId, endLabelId );
                long value = state.incrementRelationshipCount( key, delta );
                assert value >= 0 : String.format( "incrementRelationshipCount(key=%s, delta=%d) -> value=%d", key, delta, value );
                return value;
            }
        }
    }

    @Override
    public long indexSizeCount( int labelId, int propertyKeyId )
    {
        return state.indexSizeCount( indexSizeKey( labelId, propertyKeyId ) );
    }

    @Override
    public long incrementIndexSizeCount( int labelId, int propertyKeyId, long delta )
    {
        if ( delta == 0 )
        {
            return indexSizeCount( labelId, propertyKeyId );
        }
        {
            try ( LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
            {
                CountsKey.IndexSizeKey key = indexSizeKey( labelId, propertyKeyId );
                long value = state.incrementIndexSizeCount( key, delta );
                assert value >= 0 : String.format( "incrementIndexSizeCount(key=%s, delta=%d) -> value=%d", key, delta, value );
                return value;
            }
        }
    }

    @Override
    public boolean indexSample( int labelId, int propertyKeyId, Register.DoubleLongRegister target )
    {
        return state.indexSample( indexSampleKey( labelId, propertyKeyId ), target );
    }

    @Override
    public void replaceIndexSizeCount( int labelId, int propertyKeyId, long total )
    {
        try ( LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
        {
            CountsKey.IndexSizeKey key = indexSizeKey( labelId, propertyKeyId );
            assert total >= 0 : String.format( "replaceIndexSizeCount(key=%s, total=%d)", key, total );
            state.replaceIndexSizeCount( key, total );
        }
    }

    @Override
    public void replaceIndexSample( int labelId, int propertyKeyId, long unique, long size )
    {
        try ( LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
        {
            CountsKey.IndexSampleKey key = indexSampleKey( labelId, propertyKeyId );
            assert unique >= 0 && size >= 0 && unique < size : String.format( "replaceIndexSample(key=%s, unique=%d, size=%d)", key, unique, size );
            state.replaceIndexSample( key, unique, size );
        }
    }

    public void accept( final CountsVisitor visitor )
    {
        state.accept( new KeyValueRecordVisitor<CountsKey, Register.DoubleLongRegister>()
        {
            private final Register.DoubleLongRegister valueRegister = Registers.newDoubleLongRegister();

            @Override
            public Register.DoubleLongRegister valueRegister()
            {
                return valueRegister;
            }

            @Override
            public void visit( CountsKey key )
            {
                key.accept( visitor, valueRegister );
            }
        } );
    }

    public void close()
    {
        try
        {
            if ( state.hasChanges() )
            {
                throw new IllegalStateException( "Cannot close with memory-state!" );
            }
            state.close();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    public void rotate( long lastCommittedTxId ) throws IOException
    {
        try ( LockWrapper _ = new LockWrapper( updateLock.writeLock() ) )
        {
            CountsTrackerState state = this.state;
            if ( state.hasChanges() )
            {
                // select the next file, and create a writer for it
                try ( CountsStore.Writer<CountsKey, Register.DoubleLongRegister> writer =
                              nextWriter( state, lastCommittedTxId ) )
                {
                    state.accept( writer );
                    // replaceSecond the old store with the
                    this.state = new ConcurrentCountsTrackerState( writer.openForReading() );
                }
                // close the old store
                state.close();
            }
        }
    }

    CountsStore.Writer<CountsKey, Register.DoubleLongRegister> nextWriter( CountsTrackerState state, long lastCommittedTxId )
            throws IOException
    {
        if ( alphaFile.equals( state.storeFile() ) )
        {
            return state.newWriter( betaFile, lastCommittedTxId );
        }
        else
        {
            return state.newWriter( alphaFile, lastCommittedTxId );
        }
    }

    File storeFile()
    {
        return state.storeFile();
    }

    private static File storeFile( File base, String version )
    {
        return new File( base.getParentFile(), base.getName() + version );
    }
}
