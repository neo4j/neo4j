/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;
import org.neo4j.kernel.impl.store.kvstore.AbstractKeyValueStore;
import org.neo4j.kernel.impl.store.kvstore.CollectedMetadata;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.Rotation;
import org.neo4j.kernel.impl.store.kvstore.WritableBuffer;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.register.Register;

import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexSampleKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.indexStatisticsKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.nodeKey;
import static org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory.relationshipKey;

/**
 * This is the main class for the counts store.
 *
 * The counts store is a key/value store, where key/value entries are stored sorted by the key in ascending unsigned
 * (big endian) order. These store files are immutable, and on store-flush the implementation swaps the read and write
 * file in a {@linkplain Rotation.Strategy#LEFT_RIGHT left/right pattern}.
 *
 * This class defines {@linkplain CountsTracker.KeyFormat the key serialisation format},
 * {@linkplain CountsTracker.ValueFormat the value serialisation format}, and {@linkplain Metadata the metadata format}.
 *
 * The {@linkplain AbstractKeyValueStore parent class} defines the life cycle of the store.
 *
 * The pattern of immutable store files, and rotation strategy, et.c. is defined in the
 * {@code kvstore}-package, see {@link org.neo4j.kernel.impl.store.kvstore.KeyValueStoreFile} for a good entry point.
 */
@Rotation(value = Rotation.Strategy.LEFT_RIGHT, parameters = {CountsTracker.LEFT, CountsTracker.RIGHT})
public class CountsTracker extends AbstractKeyValueStore<CountsKey, Metadata, Metadata.Diff>
        implements CountsVisitor.Visitable, CountsAccessor
{
    /** The format specifier for the current version of the store file format. */
    private static final byte[] FORMAT = {'N', 'e', 'o', 'C', 'o', 'u', 'n', 't',
                                          'S', 't', 'o', 'r', 'e', /**/0, 0, 'V'};
    public static final String LEFT = ".a", RIGHT = ".b";
    public static final String TYPE_DESCRIPTOR = "CountsStore";
    private final StringLogger logger;

    public CountsTracker( StringLogger logger, FileSystemAbstraction fs, PageCache pages, File baseFile )
    {
        super( fs, pages, baseFile, 16, 16, Metadata.KEYS );
        this.logger = logger;
    }

    public void rotate( long txId ) throws IOException
    {
        logger.debug( "Start writing new counts store with txId=" + txId );
        rotate( new Metadata.Diff( txId ) );
        logger.debug( "Completed writing of counts store with txId=" + txId );
    }

    public boolean acceptTx( long txId )
    {
        Metadata metadata = metadata();
        return metadata != null && metadata.txId < txId;
    }

    public long txId()
    {
        return metadata().txId;
    }

    public long minorVersion()
    {
        return metadata().minorVersion;
    }

    public Register.DoubleLongRegister get( CountsKey key, Register.DoubleLongRegister target )
    {
        try
        {
            return lookup( key, new ValueRegister( target ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public Register.DoubleLongRegister nodeCount( int labelId, final Register.DoubleLongRegister target )
    {
        return get( nodeKey( labelId ), target );
    }

    @Override
    public Register.DoubleLongRegister relationshipCount( int startLabelId, int typeId, int endLabelId,
                                                          Register.DoubleLongRegister target )
    {
        return get( relationshipKey( startLabelId, typeId, endLabelId ), target );
    }

    @Override
    public Register.DoubleLongRegister indexUpdatesAndSize( int labelId, int propertyKeyId,
                                                            Register.DoubleLongRegister target )
    {
        return get( indexStatisticsKey( labelId, propertyKeyId ), target );
    }

    @Override
    public Register.DoubleLongRegister indexSample( int labelId, int propertyKeyId, Register.DoubleLongRegister target )
    {
        return get( indexSampleKey( labelId, propertyKeyId ), target );
    }

    /**
     * For key format, see {@link KeyFormat#visitIndexStatistics(int, int, long, long)}
     * For value format, see {@link ValueFormat#replaceIndexUpdateAndSize(int, int, long, long)}
     */
    @Override
    public void incrementIndexUpdates( int labelId, int propertyKeyId, long delta )
    {
        try
        {
            apply( new UpdateFirstValue( indexStatisticsKey( labelId, propertyKeyId ), delta ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    public CountsAccessor.Updater updater()
    {
        return new ValueFormat();
    }

    @Override
    public void accept( final CountsVisitor visitor )
    {
        try
        {
            visitAll( new Visitor()
            {
                @Override
                protected boolean visitKeyValuePair( CountsKey key, ReadableBuffer value )
                {
                    key.accept( visitor, value.getLong( 0 ), value.getLong( 8 ) );
                    return true;
                }
            } );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    @Override
    protected Metadata initialMetadata()
    {
        return new Metadata( -1, 1 );
    }

    @Override
    protected int compareMetadata( Metadata lhs, Metadata rhs )
    {
        return compare( lhs, rhs );
    }

    static int compare( Metadata lhs, Metadata rhs )
    {
        int cmp = Long.compare( lhs.txId, rhs.txId );
        if ( cmp == 0 )
        {
            cmp = Long.compare( lhs.minorVersion, rhs.minorVersion );
        }
        return cmp;
    }

    @Override
    protected Metadata updateMetadata( Metadata metadata, Metadata.Diff changes )
    {
        return metadata.update( changes );
    }

    @Override
    protected void writeKey( CountsKey key, final WritableBuffer buffer )
    {
        key.accept( new KeyFormat( buffer ), 0, 0 );
    }

    @Override
    protected CountsKey readKey( ReadableBuffer key )
    {
        switch ( key.getByte( 0 ) )
        {
        case KeyFormat.NODE_COUNT:
            return CountsKeyFactory.nodeKey( key.getInt( 12 ) );
        case KeyFormat.RELATIONSHIP_COUNT:
            return CountsKeyFactory.relationshipKey( key.getInt( 4 ), key.getInt( 8 ), key.getInt( 12 ) );
        case KeyFormat.INDEX:
            switch ( key.getByte( 15 ) )
            {
            case KeyFormat.INDEX_STATS:
                return indexStatisticsKey( key.getInt( 4 ), key.getInt( 8 ) );
            case KeyFormat.INDEX_SAMPLE:
                return CountsKeyFactory.indexSampleKey( key.getInt( 4 ), key.getInt( 8 ) );
            }
        default:
            throw new IllegalArgumentException( "Unknown key type: " + key );
        }
    }

    @Override
    protected Metadata buildMetadata( ReadableBuffer formatSpecifier, CollectedMetadata metadata )
    {
        Metadata.TxId txId = metadata.getMetadata( Metadata.TX_ID );
        return new Metadata( txId.txId, txId.minorVersion );
    }

    @Override
    protected String extractFileTrailer( Metadata metadata )
    {
        return StoreFactory.buildTypeDescriptorAndVersion( TYPE_DESCRIPTOR );
    }

    @Override
    protected boolean include( CountsKey countsKey, ReadableBuffer value )
    {
        return !value.allZeroes();
    }

    @Override
    protected void failedToOpenStoreFile( File path, Exception error )
    {
        logger.logMessage( "Failed to open counts store file: " + path, error );
    }

    protected void beforeRotation( File source, File target, Metadata metadata )
    {
        logger.logMessage( String.format( "About to rotate counts store at transaction %d to [%s], from [%s].",
                                          metadata.txId, target, source ) );
    }

    @Override
    protected void rotationSucceeded( File source, File target, Metadata metadata )
    {
        logger.logMessage( String.format( "Successfully rotated counts store at transaction %d to [%s], from [%s].",
                                          metadata.txId, target, source ) );
    }

    @Override
    protected void rotationFailed( File source, File target, Metadata metadata, Exception e )
    {
        logger.logMessage( String.format( "Failed to rotate counts store at transaction %d to [%s], from [%s].",
                                          metadata.txId, target, source ), e );
    }

    @Override
    protected void writeFormatSpecifier( WritableBuffer formatSpecifier )
    {
        formatSpecifier.put( 0, FORMAT );
    }

    @Override
    protected boolean hasMetadataChanges( Metadata metadata, Metadata.Diff diff )
    {
        return metadata != null && metadata.txId != diff.txId;
    }

    private static class KeyFormat implements CountsVisitor
    {
        private static final byte NODE_COUNT = 1, RELATIONSHIP_COUNT = 2, INDEX = 127, INDEX_STATS = 1, INDEX_SAMPLE = 2;
        private final WritableBuffer buffer;

        public KeyFormat( WritableBuffer key )
        {
            assert key.size() >= 16;
            this.buffer = key;
        }

        /**
         * Key format:
         * <pre>
         *  0 1 2 3 4 5 6 7   8 9 A B C D E F
         * [t,0,0,0,0,0,0,0 ; 0,0,0,0,l,l,l,l]
         *  t - entry type - "{@link #NODE_COUNT}"
         *  l - label id
         * </pre>
         * For value format, see {@link ValueFormat#incrementNodeCount(int, long)}.
         */
        @Override
        public void visitNodeCount( int labelId, long count )
        {
            buffer.putByte( 0, NODE_COUNT )
                  .putInt( 12, labelId );
        }

        /**
         * Key format:
         * <pre>
         *  0 1 2 3 4 5 6 7   8 9 A B C D E F
         * [t,0,0,0,s,s,s,s ; r,r,r,r,e,e,e,e]
         *  t - entry type - "{@link #RELATIONSHIP_COUNT}"
         *  s - start label id
         *  r - relationship type id
         *  e - end label id
         * </pre>
         * For value format, see {@link ValueFormat#incrementRelationshipCount(int, int, int, long)}
         */
        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            buffer.putByte( 0, RELATIONSHIP_COUNT )
                  .putInt( 4, startLabelId )
                  .putInt( 8, typeId )
                  .putInt( 12, endLabelId );
        }

        /**
         * Key format:
         * <pre>
         *  0 1 2 3 4 5 6 7   8 9 A B C D E F
         * [t,0,0,0,l,l,l,l ; p,p,p,p,0,0,0,k]
         *  t - index entry marker - "{@link #INDEX}"
         *  k - entry (sub)type - "{@link #INDEX_STATS}"
         *  l - label id
         *  p - property key id
         * </pre>
         * For value format, see {@link ValueFormat#replaceIndexUpdateAndSize(int, int, long, long)}.
         */
        @Override
        public void visitIndexStatistics( int labelId, int propertyKeyId, long updates, long size )
        {
            indexKey( INDEX_STATS, labelId, propertyKeyId );
        }

        /**
         * Key format:
         * <pre>
         *  0 1 2 3 4 5 6 7   8 9 A B C D E F
         * [t,0,0,0,l,l,l,l ; p,p,p,p,0,0,0,k]
         *  t - index entry marker - "{@link #INDEX}"
         *  k - entry (sub)type - "{@link #INDEX_SAMPLE}"
         *  l - label id
         *  p - property key id
         * </pre>
         * For value format, see {@link ValueFormat#replaceIndexSample(int, int, long, long)}.
         */
        @Override
        public void visitIndexSample( int labelId, int propertyKeyId, long unique, long size )
        {
            indexKey( INDEX_SAMPLE, labelId, propertyKeyId );
        }

        private void indexKey( byte indexKey, int labelId, int propertyKeyId )
        {
            buffer.putByte( 0, INDEX )
                  .putInt( 4, labelId )
                  .putInt( 8, propertyKeyId )
                  .putByte( 15, indexKey );
        }
    }

    private class ValueFormat extends AbstractKeyValueStore<CountsKey, ?, ?>.Updater implements CountsAccessor.Updater
    {
        /**
         * Value format:
         * <pre>
         *  0 1 2 3 4 5 6 7   8 9 A B C D E F
         * [0,0,0,0,0,0,0,0 ; c,c,c,c,c,c,c,c]
         *  c - number of matching nodes
         * </pre>
         * For key format, see {@link KeyFormat#visitNodeCount(int, long)}
         */
        @Override
        public void incrementNodeCount( int labelId, final long delta )
        {
            try
            {
                apply( new UpdateSecondValue( nodeKey( labelId ), delta ) );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }

        /**
         * Value format:
         * <pre>
         *  0 1 2 3 4 5 6 7   8 9 A B C D E F
         * [0,0,0,0,0,0,0,0 ; c,c,c,c,c,c,c,c]
         *  c - number of matching relationships
         * </pre>
         * For key format, see {@link KeyFormat#visitRelationshipCount(int, int, int, long)}
         */
        @Override
        public void incrementRelationshipCount( int startLabelId, int typeId, int endLabelId, long delta )
        {
            try
            {
                apply( new UpdateSecondValue( relationshipKey( startLabelId, typeId, endLabelId ), delta ) );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }

        /**
         * Value format:
         * <pre>
         *  0 1 2 3 4 5 6 7   8 9 A B C D E F
         * [u,u,u,u,u,u,u,u ; s,s,s,s,s,s,s,s]
         *  u - number of updates
         *  s - size of index
         * </pre>
         * For key format, see {@link KeyFormat#visitIndexStatistics(int, int, long, long)}
         */
        @Override
        public void replaceIndexUpdateAndSize( int labelId, int propertyKeyId, final long updates, final long size )
        {
            try
            {
                apply( new AssignValues( indexStatisticsKey( labelId, propertyKeyId ), updates, size ) );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }

        /**
         * Value format:
         * <pre>
         *  0 1 2 3 4 5 6 7   8 9 A B C D E F
         * [u,u,u,u,u,u,u,u ; s,s,s,s,s,s,s,s]
         *  u - number of unique values
         *  s - size of index
         * </pre>
         * For key format, see {@link KeyFormat#visitIndexSample(int, int, long, long)}
         */
        @Override
        public void replaceIndexSample( int labelId, int propertyKeyId, final long unique, final long size )
        {
            try
            {
                apply( new AssignValues( indexSampleKey( labelId, propertyKeyId ), unique, size ) );
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( e );
            }
        }
    }
}
