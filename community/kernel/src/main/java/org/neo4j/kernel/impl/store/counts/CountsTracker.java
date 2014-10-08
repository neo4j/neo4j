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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.CountsAcceptor;
import org.neo4j.kernel.impl.api.CountsKey;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.locking.LockWrapper;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;

import static org.neo4j.kernel.impl.api.CountsKey.nodeKey;
import static org.neo4j.kernel.impl.api.CountsKey.relationshipKey;

/**
 * {@link CountsTracker} maintains two files, the {@link #alphaFile} and the {@link #betaFile} that it rotates between.
 * {@link #updateLock} is used to ensure that no updates happen while we rotate from one file to another. Reads are
 * still ok though, they just read whatever the current state is. The state is assigned atomically at the end of
 * rotation.
 */
public class CountsTracker implements CountsVisitor.Visitable, AutoCloseable, CountsAcceptor
{
    public static final String STORE_DESCRIPTOR = CountsStore.class.getSimpleName();

    interface State extends Closeable
    {
        long lastTxId();

        boolean hasChanges();

        long getCount( CountsKey key );

        long updateCount( CountsKey key, long delta );

        File storeFile();

        CountsStore.Writer newWriter( File file, long lastCommittedTxId ) throws IOException;

        void accept( RecordVisitor visitor );
    }

    public static final String ALPHA = ".alpha", BETA = ".beta";
    private final File alphaFile, betaFile;
    private final ReadWriteLock updateLock = new ReentrantReadWriteLock( /*fair=*/true );
    private volatile State state;

    public CountsTracker( FileSystemAbstraction fs, PageCache pageCache, File storeFileBase )
    {
        this.alphaFile = storeFile( storeFileBase, ALPHA );
        this.betaFile = storeFile( storeFileBase, BETA );
        this.state = new ConcurrentTrackerState( openStore( fs, pageCache, this.alphaFile, this.betaFile ) );
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
                long alphaTxId = alphaStore.lastTxId(), betaTxId = betaStore.lastTxId();
                if ( alphaTxId > betaTxId )  // TODO: compare to what the txIdProvider says...
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

    public static void createEmptyCountsStore( PageCache pageCache, File file, String version )
    {
        CountsStore.createEmpty( pageCache, storeFile( file, ALPHA ), version );
    }

    public long countsForNode( int labelId )
    {
        return get( nodeKey( labelId ) );
    }

    public boolean acceptTx( long txId )
    {
        return state.lastTxId() < txId;
    }

    @Override
    public void updateCountsForNode( int labelId, long delta )
    {
        update( nodeKey( labelId ), delta );
    }

    public long countsForRelationship( int startLabelId, int typeId, int endLabelId )
    {
        return get( relationshipKey( startLabelId, typeId, endLabelId ) );
    }

    @Override
    public void updateCountsForRelationship( int startLabelId, int typeId, int endLabelId, long delta )
    {
        update( relationshipKey( startLabelId, typeId, endLabelId ), delta );
    }

    public void accept( final CountsVisitor visitor )
    {
        state.accept( new RecordVisitor()
        {
            @Override
            public void visit( CountsKey key, long value )
            {
                key.accept( visitor, value );
            }
        } );
    }

    private long get( CountsKey key )
    {
        return state.getCount( key );
    }

    private void update( CountsKey key, long delta )
    {
        if ( delta != 0 )
        {
            try ( LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
            {
                long value = state.updateCount( key, delta );
                assert value >= 0 : String.format( "update(key=%s, delta=%d) -> value=%d", key, delta, value );
            }
        }
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
            State state = this.state;
            if ( state.hasChanges() )
            {
                // select the next file, and create a writer for it
                try ( CountsStore.Writer writer = nextWriter( state, lastCommittedTxId ) )
                {
                    state.accept( writer );
                    // replace the old store with the
                    this.state = new ConcurrentTrackerState( writer.openForReading( ) );
                }
                // close the old store
                state.close();
            }
        }
    }

    CountsStore.Writer nextWriter( State state, long lastCommittedTxId ) throws IOException
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

    private static File storeFile( File base, String version )
    {
        return new File( base.getParentFile(), base.getName() + version );
    }
}
