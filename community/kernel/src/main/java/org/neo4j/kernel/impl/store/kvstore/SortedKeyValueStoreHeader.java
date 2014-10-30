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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.IOException;
import java.util.Arrays;

import org.neo4j.helpers.UTF8;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static java.lang.String.format;
import static org.neo4j.io.pagecache.PagedFile.PF_EXCLUSIVE_LOCK;
import static org.neo4j.io.pagecache.PagedFile.PF_SHARED_LOCK;
import static org.neo4j.kernel.impl.store.kvstore.SortedKeyValueStore.RECORD_SIZE;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public final class SortedKeyValueStoreHeader
{
    public static final long BASE_MINOR_VERSION = 1l;

    public static SortedKeyValueStoreHeader empty( String storeFormatVersion )
    {
        if ( storeFormatVersion == null )
        {
            throw new IllegalArgumentException( "store format version cannot be null" );
        }
        return new SortedKeyValueStoreHeader( UTF8.encode( storeFormatVersion ), 0, BASE_TX_ID, BASE_MINOR_VERSION );
    }

    private static final int META_HEADER_SIZE =
            2 /*headerRecords*/ + 2 /*versionLen*/ + 4 /*dataRecords*/ + 8 /*lastTxId*/ + 8 /*minorVersion*/;

    private final byte[] storeFormatVersion;
    private final int dataRecords;
    private final long lastTxId;
    private final long minorVersion;

    private SortedKeyValueStoreHeader( byte[] storeFormatVersion, int dataRecords, long lastTxId, long minorVersion )
    {
        this.storeFormatVersion = storeFormatVersion;
        this.dataRecords = dataRecords;
        this.lastTxId = lastTxId;
        this.minorVersion = minorVersion;
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

        SortedKeyValueStoreHeader that = (SortedKeyValueStoreHeader) o;

        return dataRecords == that.dataRecords &&
                lastTxId == that.lastTxId &&
                minorVersion == that.minorVersion &&
                Arrays.equals( storeFormatVersion, that.storeFormatVersion );
    }

    @Override
    public int hashCode()
    {
        int result = Arrays.hashCode( storeFormatVersion );
        result = 31 * result + dataRecords;
        result = 31 * result + (int) (lastTxId ^ (lastTxId >>> 32));
        result = 31 * result + (int) (minorVersion ^ (minorVersion >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "%s[storeFormatVersion=%s, dataRecords=%d, lastTxId=%d, minorVersion=%d]",
                              getClass().getSimpleName(), storeFormatVersion(), dataRecords, lastTxId, minorVersion );
    }

    public SortedKeyValueStoreHeader update( int dataRecords, long lastTxId, long minorVersion )
    {
        return new SortedKeyValueStoreHeader( storeFormatVersion, dataRecords, lastTxId, minorVersion );
    }

    String storeFormatVersion()
    {
        return UTF8.decode( storeFormatVersion );
    }

    public int headerRecords()
    {
        int headerBytes = META_HEADER_SIZE + storeFormatVersion.length;
        headerBytes += RECORD_SIZE - (headerBytes % RECORD_SIZE);
        return headerBytes / RECORD_SIZE;
    }

    public int dataRecords()
    {
        return dataRecords;
    }

    public long lastTxId()
    {
        return lastTxId;
    }

    public long minorVersion()
    {
        return minorVersion;
    }

    public static SortedKeyValueStoreHeader read( PagedFile pagedFile ) throws IOException
    {
        try ( PageCursor page = pagedFile.io( 0, PF_SHARED_LOCK ) )
        {
            if ( page.next() )
            {
                short headerRecords;
                short versionLength;
                int dataRecords;
                long lastTxId;
                long minorVersion;
                byte[] storeFormatVersion = null;
                int versionSpace = -1;
                byte[] tail = null;
                do
                {
                    page.setOffset( 0 );
                    headerRecords = page.getShort();
                    versionLength = page.getShort();
                    dataRecords = page.getInt();
                    lastTxId = page.getLong();
                    minorVersion = page.getLong();
                    // go on only if read data are meaningful
                    if ( versionLength >= 0 )
                    {
                        storeFormatVersion = new byte[versionLength];
                        page.getBytes( storeFormatVersion );
                        versionSpace = headerRecords * RECORD_SIZE - META_HEADER_SIZE;
                        int tailLength = versionSpace - versionLength - 1;
                        // go on only if read data are meaningful
                        if ( tailLength >= 0 )
                        {
                            tail = new byte[tailLength];
                            page.getBytes( tail );
                        }
                    }
                } while ( page.shouldRetry() );

                checkConsistentHeader( versionLength, versionSpace );
                checkZeroPadded( tail );

                return new SortedKeyValueStoreHeader( storeFormatVersion, dataRecords, lastTxId, minorVersion );
            }
            else
            {
                throw new IOException( "Could not read count store header page" );
            }
        }
    }

    private static void checkConsistentHeader( short versionLength, int versionSpace ) throws IOException
    {
        if ( versionLength < 0 && versionLength > versionSpace || versionLength < (versionSpace - RECORD_SIZE) )
        {
            throw new IOException(
                    format( "Invalid header data, versionLength=%d, versionSpace=%d.", versionLength, versionSpace )
            );
        }
    }

    private static void checkZeroPadded( byte[] tail ) throws IOException
    {
        if ( tail == null )
        {
            throw new IOException( "Unexpected header data." );
        }


        for ( int i = 0; i < tail.length; i++ )
        {
            if ( tail[i] != 0 )
            {
                throw new IOException( "Unexpected header data." );
            }
        }
    }

    public void write( PagedFile pagedFile ) throws IOException
    {
        try ( PageCursor page = pagedFile.io( 0, PF_EXCLUSIVE_LOCK ) )
        {
            if ( page.next() )
            {
                write( page );
            }
            else
            {
                throw new IOException( "Could not write count store header page" );
            }
        }
    }

    public void write( PageCursor page ) throws IOException
    {
        do
        {
            page.setOffset( 0 );
            page.putShort( (short) headerRecords() );
            page.putShort( (short) storeFormatVersion.length );
            page.putInt( dataRecords );
            page.putLong( lastTxId );
            page.putLong( minorVersion );
            page.putBytes( storeFormatVersion );
            page.setOffset( RECORD_SIZE * headerRecords() );
        } while ( page.shouldRetry() );
    }
}
