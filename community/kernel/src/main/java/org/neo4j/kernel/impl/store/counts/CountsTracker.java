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
package org.neo4j.kernel.impl.store.counts;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Clock;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.api.CountsVisitor;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.store.counts.keys.CountsKey;
import org.neo4j.kernel.impl.store.kvstore.AbstractKeyValueStore;
import org.neo4j.kernel.impl.store.kvstore.DataInitializer;
import org.neo4j.kernel.impl.store.kvstore.EntryUpdater;
import org.neo4j.kernel.impl.store.kvstore.HeaderField;
import org.neo4j.kernel.impl.store.kvstore.Headers;
import org.neo4j.kernel.impl.store.kvstore.MetadataVisitor;
import org.neo4j.kernel.impl.store.kvstore.ReadableBuffer;
import org.neo4j.kernel.impl.store.kvstore.Rotation;
import org.neo4j.kernel.impl.store.kvstore.RotationMonitor;
import org.neo4j.kernel.impl.store.kvstore.RotationTimerFactory;
import org.neo4j.kernel.impl.store.kvstore.UnknownKey;
import org.neo4j.kernel.impl.store.kvstore.WritableBuffer;
import org.neo4j.kernel.impl.util.function.Optional;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.register.Register;

import static java.lang.String.format;
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
 * This class defines {@linkplain KeyFormat the key serialisation format},
 * {@linkplain CountsUpdater the value serialisation format}, and
 * {@linkplain #HEADER_FIELDS the header fields}.
 *
 * The {@linkplain AbstractKeyValueStore parent class} defines the life cycle of the store.
 *
 * The pattern of immutable store files, and rotation strategy, et.c. is defined in the
 * {@code kvstore}-package, see {@link org.neo4j.kernel.impl.store.kvstore.KeyValueStoreFile} for a good entry point.
 */
@Rotation(value = Rotation.Strategy.LEFT_RIGHT, parameters = {CountsTracker.LEFT, CountsTracker.RIGHT})
public class CountsTracker extends AbstractKeyValueStore<CountsKey>
        implements CountsVisitor.Visitable, CountsAccessor
{
    /** The format specifier for the current version of the store file format. */
    private static final byte[] FORMAT = {'N', 'e', 'o', 'C', 'o', 'u', 'n', 't',
                                          'S', 't', 'o', 'r', 'e', /**/0, 1, 'V'};
    @SuppressWarnings("unchecked")
    private static final HeaderField<?>[] HEADER_FIELDS = new HeaderField[]{FileVersion.FILE_VERSION};
    public static final String LEFT = ".a", RIGHT = ".b";
    public static final String TYPE_DESCRIPTOR = "CountsStore";

    public CountsTracker( final LogProvider logProvider, FileSystemAbstraction fs, PageCache pages, Config config,
            File baseFile )
    {
        super( fs, pages, baseFile, new RotationMonitor()
        {
            final Log log = logProvider.getLog( CountsTracker.class );

            @Override
            public void failedToOpenStoreFile( File path, Exception error )
            {
                log.error( "Failed to open counts store file: " + path, error );
            }

            @Override
            public void beforeRotation( File source, File target, Headers headers )
            {
                log.info( format( "About to rotate counts store at transaction %d to [%s], from [%s].",
                        headers.get( FileVersion.FILE_VERSION ).txId, target, source ) );
            }

            @Override
            public void rotationSucceeded( File source, File target, Headers headers )
            {
                log.info( format( "Successfully rotated counts store at transaction %d to [%s], from [%s].",
                        headers.get( FileVersion.FILE_VERSION ).txId, target, source ) );
            }

            @Override
            public void rotationFailed( File source, File target, Headers headers, Exception e )
            {
                log.error( format( "Failed to rotate counts store at transaction %d to [%s], from [%s].",
                        headers.get( FileVersion.FILE_VERSION ).txId, target, source ), e );
            }
        }, new RotationTimerFactory( Clock.SYSTEM_CLOCK,
                config.get( GraphDatabaseSettings.store_interval_log_rotation_wait_time ) ), 16, 16, HEADER_FIELDS );
    }

    public CountsTracker setInitializer( final DataInitializer<Updater> initializer )
    {
        setEntryUpdaterInitializer( new DataInitializer<EntryUpdater<CountsKey>>()
        {
            @Override
            public void initialize( EntryUpdater<CountsKey> updater )
            {
                initializer.initialize( new CountsUpdater( updater ) );
            }

            @Override
            public long initialVersion()
            {
                return initializer.initialVersion();
            }
        } );
        return this;
    }

    /**
     * @param txId the lowest transaction id that must be included in the snapshot created by the rotation.
     * @return the highest transaction id that was included in the snapshot created by the rotation.
     */
    public long rotate( long txId ) throws IOException
    {
        return prepareRotation( txId ).rotate();
    }

    public long txId()
    {
        return headers().get( FileVersion.FILE_VERSION ).txId;
    }

    public long minorVersion()
    {
        return headers().get( FileVersion.FILE_VERSION ).minorVersion;
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

    public Optional<CountsAccessor.Updater> apply( long txId )
    {
        return updater( txId ).<CountsAccessor.Updater>map( CountsUpdater.FACTORY );
    }

    public CountsAccessor.IndexStatsUpdater updateIndexCounts()
    {
        return new CountsUpdater( updater() );
    }

    public CountsAccessor.Updater reset( long txId )
    {
        return new CountsUpdater( resetter( txId ) );
    }

    @Override
    public void accept( final CountsVisitor visitor )
    {
        try
        {
            visitAll( new DelegatingVisitor( visitor ) );
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    void visitFile( File path, CountsVisitor visitor ) throws IOException
    {
        super.visitFile( path, new DelegatingVisitor( visitor ) );
    }

    @Override
    protected Headers initialHeaders( long txId )
    {
        return Headers.headersBuilder().put( FileVersion.FILE_VERSION, new FileVersion( txId ) ).headers();
    }

    @Override
    protected int compareHeaders( Headers lhs, Headers rhs )
    {
        return compare( lhs.get( FileVersion.FILE_VERSION ), rhs.get( FileVersion.FILE_VERSION ) );
    }

    static int compare( FileVersion lhs, FileVersion rhs )
    {
        int cmp = Long.compare( lhs.txId, rhs.txId );
        if ( cmp == 0 )
        {
            cmp = Long.compare( lhs.minorVersion, rhs.minorVersion );
        }
        return cmp;
    }

    @Override
    protected void writeKey( CountsKey key, final WritableBuffer buffer )
    {
        key.accept( new KeyFormat( buffer ), 0, 0 );
    }

    @Override
    protected CountsKey readKey( ReadableBuffer key ) throws UnknownKey
    {
        return KeyFormat.readKey( key );
    }

    @Override
    protected boolean include( CountsKey countsKey, ReadableBuffer value )
    {
        return !value.allZeroes();
    }

    @Override
    protected void updateHeaders( Headers.Builder headers, long version )
    {
        headers.put( FileVersion.FILE_VERSION, headers.get( FileVersion.FILE_VERSION ).update( version ) );
    }

    @Override
    protected long version( Headers headers )
    {
        return headers == null ? FileVersion.INITIAL_TX_ID : headers.get( FileVersion.FILE_VERSION ).txId;
    }

    @Override
    protected void writeFormatSpecifier( WritableBuffer formatSpecifier )
    {
        formatSpecifier.put( 0, FORMAT );
    }

    private class DelegatingVisitor extends Visitor implements MetadataVisitor
    {
        private final CountsVisitor visitor;

        public DelegatingVisitor( CountsVisitor visitor )
        {
            this.visitor = visitor;
        }

        @Override
        protected boolean visitKeyValuePair( CountsKey key, ReadableBuffer value )
        {
            key.accept( visitor, value.getLong( 0 ), value.getLong( 8 ) );
            return true;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void visitMetadata( File path, Headers headers, int entryCount )
        {
            if ( visitor instanceof MetadataVisitor )
            {
                ((MetadataVisitor) visitor).visitMetadata( path, headers, entryCount );
            }
        }

        @Override
        protected boolean visitUnknownKey( UnknownKey exception, ReadableBuffer key, ReadableBuffer value )
        {
            if ( visitor instanceof UnknownKey.Visitor )
            {
                return ((UnknownKey.Visitor) visitor).visitUnknownKey( key, value );
            }
            else
            {
                return super.visitUnknownKey( exception, key, value );
            }
        }
    }
}
