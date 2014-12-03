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
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.KeyValueRecordVisitor;
import org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.register.Register.CopyableDoubleLongRegister;
import org.neo4j.register.Register.DoubleLongRegister;
import org.neo4j.register.Registers;

import static org.neo4j.kernel.impl.store.counts.CountsStore.RECORD_SIZE;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexCountsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader.BASE_MINOR_VERSION;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStoreHeader.with;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

/**
 * {@link CountsTracker} maintains two files, the {@link #leftFile} and the {@link #rightFile} that it rotates between.
 * {@link #updateLock} is used to ensure that no updates happen while we rotate from one file to another. Reads are
 * still ok though, they just read whatever the current state is. The state is assigned atomically at the end of
 * rotation.
 */
public class CountsTracker implements CountsVisitor.Visitable, AutoCloseable, CountsAccessor
{
    public static final String STORE_DESCRIPTOR = SortedKeyValueStore.class.getSimpleName();

    public static final String LEFT = ".left", RIGHT = ".right";
    private final File leftFile, rightFile;
    private final ReadWriteLock updateLock = new ReentrantReadWriteLock( /*fair=*/true );
    private final StringLogger logger;
    private volatile CountsTrackerState state;

    public CountsTracker( StringLogger logger, FileSystemAbstraction fs, PageCache pageCache,
                          File storeFileBase, long neoStoreTxId )
    {
        this.logger = logger;
        this.leftFile = storeFile( storeFileBase, LEFT );
        this.rightFile = storeFile( storeFileBase, RIGHT );
        CountsStore store = openStore( logger, fs, pageCache, this.leftFile, this.rightFile );
        if ( store.lastTxId() < neoStoreTxId )
        {
            IOException exOnClose = safelyCloseTheStore( store );
            throw new UnderlyingStorageException(
                "Counts store seems to be out of date ( last count store txid is " + store.lastTxId() +
                        " but database wide last txid is " + neoStoreTxId +
                        " ). Please shut down the database and manually delete the counts store files " +
                "to have the database recreate them on next startup", exOnClose );
        }
        this.state = new ConcurrentCountsTrackerState( store );
    }

    private static CountsStore openStore( StringLogger logger, FileSystemAbstraction fs, PageCache pageCache,
                                          File left, File right )
    {
        try
        {
            CountsStore leftStore = openVerifiedCountsStore( fs, pageCache, left );
            CountsStore rightStore = openVerifiedCountsStore( fs, pageCache, right );

            boolean isLeftCorrupted = leftStore == null;
            boolean isRightCorrupted = rightStore == null;
            if ( isLeftCorrupted && isRightCorrupted )
            {
                throw new UnderlyingStorageException(
                        "Neither of the two store files could be properly opened. Please shut down the database and delete them " +
                        "to have the database recreate the counts store on next startup" );
            }

            if ( isLeftCorrupted )
            {
                logger.debug( "CountsStore picked right store file since left store file could not be opened " +
                              "(txId=" + rightStore.lastTxId() +
                              ", minorVersion=" + rightStore.minorVersion() + ")" );
                return rightStore;
            }

            if ( isRightCorrupted )
            {
                logger.debug( "CountsStore picked left store file since right store file could not be opened " +
                              "(txId=" + leftStore.lastTxId() +
                              ", minorVersion=" + leftStore.minorVersion() + ")" );
                return leftStore;
            }

            // default case
            if ( isLeftStoreMoreRecent( leftStore, rightStore ) )
            {
                logger.debug( "CountsStore picked left store file (txId=" + leftStore.lastTxId() +
                              ", minorVersion=" + leftStore.minorVersion() +
                              "), against right store file (txId=" + rightStore.lastTxId() +
                              ", minorVersion=" + rightStore.minorVersion() + ")" );
                rightStore.close();
                return leftStore;
            }
            else
            {
                logger.debug( "CountsStore picked right store file (txId=" + rightStore.lastTxId() +
                              ", minorVersion=" + rightStore.minorVersion() +
                              "), against left store file (txId=" + leftStore.lastTxId() +
                              ", minorVersion=" + leftStore.minorVersion() + ")" );
                leftStore.close();
                return rightStore;
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private static CountsStore openVerifiedCountsStore( FileSystemAbstraction fs, PageCache pageCache, File file )
            throws IOException
    {
        if ( !fs.fileExists( file ) )
        {
            throw new UnderlyingStorageException(
                "Expected counts store file " + file + " to exist. You may recreate the counts store by shutting down "
              + "the database first, deleting the counts store files manually and then restarting the database");
        }

        try
        {
            return CountsStore.open( fs, pageCache, file );
        }
        catch ( UnderlyingStorageException | IOException ex )
        {
            // This will be treated as the file being corrupted, if both files are corrupted an exception will be
            // thrown from the openStore() method, informing the user that the store is corrupted, and how to fix it.
            return null;
        }
    }

    public static boolean countsStoreExists( FileSystemAbstraction fs, File storeFileBase )
    {
        final File left = storeFile( storeFileBase, LEFT );
        final File right = storeFile( storeFileBase, RIGHT );
        return fs.fileExists( left ) || fs.fileExists( right );
    }

    private static boolean isLeftStoreMoreRecent( CountsStore leftStore, CountsStore rightStore )
    {
        long leftTxId = leftStore.lastTxId(), rightTxId = rightStore.lastTxId();
        long leftMinorVersion = leftStore.minorVersion(), rightMinorVersion = rightStore.minorVersion();
        if ( leftTxId == rightTxId )
        {
            if ( leftMinorVersion == rightMinorVersion )
            {
                throw new UnderlyingStorageException( "Found two storage files with same tx id and minor version" );
            }
            return leftMinorVersion > rightMinorVersion;
        }
        else
        {
            return leftTxId > rightTxId;
        }
    }

    public static void createEmptyCountsStore( PageCache pageCache, File file, String storeVersion )
    {
        // create both files initially to avoid problems with unflushed metadata
        // increase left minor version by 1 to ensure that we use left file after creating the store

        File left = storeFile( file, LEFT );
        CountsStore.createEmpty( pageCache, left,
                with( RECORD_SIZE, storeVersion, BASE_TX_ID, BASE_MINOR_VERSION + 1 ) );

        File right = storeFile( file, RIGHT );
        CountsStore.createEmpty( pageCache, right,
                with( RECORD_SIZE, storeVersion, BASE_TX_ID, BASE_MINOR_VERSION ) );
    }

    public boolean acceptTx( long txId )
    {
        return state.lastTxId() < txId;
    }

    @Override
    public DoubleLongRegister nodeCount( int labelId, DoubleLongRegister target )
    {
        return state.nodeCount( nodeKey( labelId ), target );
    }

    @Override
    public void incrementNodeCount( int labelId, long delta )
    {
        try ( @SuppressWarnings( "UnusedDeclaration" ) LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
        {
            state.incrementNodeCount( nodeKey( labelId ), delta );
        }
    }

    @Override
    public DoubleLongRegister relationshipCount( int startLabelId, int relTypeId, int endLabelId,
                                                 DoubleLongRegister target )
    {
        return state.relationshipCount( relationshipKey( startLabelId, relTypeId, endLabelId ), target );
    }

    @Override
    public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
    {
            try ( @SuppressWarnings( "UnusedDeclaration" ) LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
            {
                state.incrementRelationshipCount( relationshipKey( startLabelId, typeId, endLabelId ), delta );
            }
    }

    @Override
    public DoubleLongRegister indexUpdatesAndSize( int labelId, int propertyKeyId, DoubleLongRegister target )
    {
        return state.indexUpdatesAndSize( indexCountsKey( labelId, propertyKeyId ), target );
    }

    @Override
    public DoubleLongRegister indexSample( int labelId, int propertyKeyId, DoubleLongRegister target )
    {
        return state.indexSample( indexSampleKey( labelId, propertyKeyId ), target );
    }

    @Override
    public void replaceIndexUpdateAndSize( int labelId, int propertyKeyId, long updates, long size )
    {
        try ( @SuppressWarnings( "UnusedDeclaration" ) LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
        {
            state.replaceIndexUpdatesAndSize( indexCountsKey( labelId, propertyKeyId ), updates, size );
        }
    }

    @Override
    public void incrementIndexUpdates( int labelId, int propertyKeyId, long delta )
    {
        try ( @SuppressWarnings( "UnusedDeclaration" ) LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
        {
            state.incrementIndexUpdates( indexCountsKey( labelId, propertyKeyId ), delta );
        }
    }

    @Override
    public void replaceIndexSample( int labelId, int propertyKeyId, long unique, long size )
    {
        try ( @SuppressWarnings( "UnusedDeclaration" ) LockWrapper _ = new LockWrapper( updateLock.readLock() ) )
        {
            state.replaceIndexSample( indexSampleKey( labelId, propertyKeyId ), unique, size );
        }
    }

    @Override
    public void accept( final CountsVisitor visitor )
    {
        state.accept( new KeyValueRecordVisitor<CountsKey,CopyableDoubleLongRegister>()
        {
            private final DoubleLongRegister target = Registers.newDoubleLongRegister();

            @Override
            public void visit( CountsKey key, CopyableDoubleLongRegister register )
            {
                register.copyTo( target );
                key.accept( visitor, target.readFirst(), target.readSecond() );
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
        try ( @SuppressWarnings( "UnusedDeclaration" ) LockWrapper _ = new LockWrapper( updateLock.writeLock() ) )
        {
            CountsTrackerState state = this.state;
            long stateTxId = state.lastTxId();
            if ( stateTxId > lastCommittedTxId )
            {
                throw new IllegalStateException( String.format(
                        "Ask to rotate on an already used txId, storeTxId=%d and got lastTxId=%d",
                        stateTxId, lastCommittedTxId ) );
            }
            if ( state.hasChanges() || stateTxId < lastCommittedTxId )
            {
                logger.debug( "Start writing new counts store with txId=" + lastCommittedTxId );
                // select the next file, and create a writer for it
                try ( CountsStore.Writer<CountsKey,CopyableDoubleLongRegister> writer =
                              nextWriter( state, lastCommittedTxId ) )
                {
                    state.accept( writer );
                    // replaceSecond the old store with the
                    this.state = new ConcurrentCountsTrackerState( writer.openForReading() );
                }
                logger.debug( "Completed writing of counts store with txId=" + lastCommittedTxId );
                // close the old store
                state.close();
            }
        }
    }

    CountsStore.Writer<CountsKey,CopyableDoubleLongRegister> nextWriter( CountsTrackerState state, long lastTxId )
            throws IOException
    {
        if ( leftFile.equals( state.storeFile() ) )
        {
            return state.newWriter( rightFile, lastTxId );
        }
        else
        {
            return state.newWriter( leftFile, lastTxId );
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

    private IOException safelyCloseTheStore( CountsStore store )
    {
        try
        {
            store.close();
            return null;
        }
        catch ( IOException ex )
        {
            return ex;
        }
    }
}
