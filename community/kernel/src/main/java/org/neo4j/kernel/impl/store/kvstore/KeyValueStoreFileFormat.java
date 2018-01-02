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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;

import static java.util.Objects.requireNonNull;

/**
 * Defines the format of a {@link KeyValueStoreFile}.
 */
public abstract class KeyValueStoreFileFormat
{
    private final int maxSize;
    private final HeaderField<?>[] headerFields;

    /**
     * @param maxSize      the largest possible size of a key or value that conforms to this format.
     * @param headerFields identifiers for the entries to write from the metadata to the store.
     */
    public KeyValueStoreFileFormat( int maxSize, HeaderField<?>... headerFields )
    {
        if ( maxSize < 0 )
        {
            throw new IllegalArgumentException( "Negative maxSize: " + maxSize );
        }
        this.maxSize = maxSize;
        this.headerFields = headerFields.clone();
    }

    public final KeyValueStoreFile createStore(
            FileSystemAbstraction fs, PageCache pages, File path, int keySize, int valueSize,
            Headers headers, DataProvider data ) throws IOException
    {
        return create( requireNonNull( fs, FileSystemAbstraction.class.getSimpleName() ),
                       requireNonNull( path, "path" ),
                       requireNonNull( pages, PageCache.class.getSimpleName() ),
                       keySize, valueSize,
                       requireNonNull( headers, "headers" ),
                       requireNonNull( data, "data" ) );
    }

    public final void createEmptyStore(
            FileSystemAbstraction fs, File path, int keySize, int valueSize, Headers headers ) throws IOException
    {
        create( requireNonNull( fs, FileSystemAbstraction.class.getSimpleName() ),
                requireNonNull( path, "path" ), null, keySize, valueSize,
                requireNonNull( headers, "headers" ), null );
    }

    public final KeyValueStoreFile openStore( FileSystemAbstraction fs, PageCache pages, File path )
            throws IOException
    {
        return open( requireNonNull( fs, FileSystemAbstraction.class.getSimpleName() ),
                     requireNonNull( path, "path" ),
                     requireNonNull( pages, PageCache.class.getSimpleName() ) );
    }

    protected abstract void writeFormatSpecifier( WritableBuffer formatSpecifier );

    protected HeaderField<?>[] headerFieldsForFormat( ReadableBuffer formatSpecifier )
    {
        return headerFields.clone();
    }

    // IMPLEMENTATION

    /** Create a collector for interpreting metadata from a file. */
    private MetadataCollector metadata( ReadableBuffer formatSpecifier, int pageSize, int keySize, int valueSize )
    {
        byte[] format = new byte[formatSpecifier.size()];
        for ( int i = 0; i < format.length; i++ )
        {
            format[i] = formatSpecifier.getByte( i );
        }
        final BigEndianByteArrayBuffer specifier = new BigEndianByteArrayBuffer( format );
        HeaderField<?>[] headerFields = headerFieldsForFormat( formatSpecifier );
        return new MetadataCollector( pageSize / (keySize + valueSize), headerFields )
        {
            @Override
            boolean verifyFormatSpecifier( ReadableBuffer value )
            {
                int size = value.size();
                if ( size == specifier.size() )
                {
                    for ( int i = 0; i < size; i++ )
                    {
                        if ( value.getByte( i ) != specifier.getByte( i ) )
                        {
                            return false;
                        }
                    }
                    return true;
                }
                return false;
            }
        };
    }

    /**
     * Create a new store file.
     *
     * @param fs           the file system that should hold the store file.
     * @param path         the location in the file system where the store file resides.
     * @param pages        if {@code null} the newly created store fill will not be opened.
     * @param keySize      the size of the keys in the new store.
     * @param valueSize    the size of the values in the new store.
     * @param headers      the headers to write to the store.
     * @param dataProvider the data to write into the store, {@code null} is accepted to mean no data.
     * @return an opened version of the newly created store file - iff a {@link PageCache} was provided.
     */
    private KeyValueStoreFile create(
            FileSystemAbstraction fs, File path, PageCache pages, int keySize, int valueSize,
            Headers headers, DataProvider dataProvider ) throws IOException
    {
        if ( keySize <= 0 || keySize > maxSize || valueSize <= 0 || valueSize > maxSize )
        {
            throw new IllegalArgumentException( String.format(
                    "Invalid sizes: keySize=%d, valueSize=%d, format maxSize=%d",
                    keySize, valueSize, maxSize ) );
        }

        if ( fs.fileExists( path ) )
        {
            fs.truncate( path, 0 );
        }

        BigEndianByteArrayBuffer key = new BigEndianByteArrayBuffer( new byte[keySize] );
        BigEndianByteArrayBuffer value = new BigEndianByteArrayBuffer( new byte[valueSize] );

        // format specifier
        writeFormatSpecifier( value );
        if ( !validFormatSpecifier( value.buffer, keySize ) )
        {
            throw new IllegalArgumentException( "Invalid Format specifier: " +
                                                BigEndianByteArrayBuffer.toString( value.buffer ) );
        }

        int pageSize = pageSize( pages, keySize, valueSize );
        try ( KeyValueWriter writer = newWriter( fs, path, value, pages, pageSize, keySize, valueSize );
              DataProvider data = dataProvider )
        {
            // header
            if ( !writer.writeHeader( key, value ) )
            {
                throw new IllegalStateException( "The format specifier should be a valid header value" );
            }
            for ( HeaderField<?> header : headerFields )
            {
                headers.write( header, value );
                if ( !writer.writeHeader( key, value ) )
                {
                    throw new IllegalArgumentException( "Invalid header value. " + header + ": " + value );
                }
            }
            if ( headerFields.length == 0 )
            {
                if ( !writer.writeHeader( key, value ) )
                {
                    throw new IllegalStateException( "A padding header should be valid." );
                }
            }

            // data
            long dataEntries = 0;
            for (; data != null && data.visit( key, value ); dataEntries++ )
            {
                writer.writeData( key, value );
            }
            // 'data' is allowed to write into the buffers even if it returns false, so we need to clear them
            key.clear();
            value.clear();

            // trailer
            value.putIntegerAtEnd( dataEntries == 0 ? -1 : dataEntries );
            if ( !writer.writeHeader( key, value ) )
            {
                throw new IllegalStateException( "The trailing size header should be valid" );
            }

            return writer.openStoreFile();
        }
    }

    private KeyValueWriter newWriter( FileSystemAbstraction fs, File path, ReadableBuffer formatSpecifier,
                                      PageCache pages, int pageSize, int keySize, int valueSize )
            throws IOException
    {
        return KeyValueWriter.create( metadata( formatSpecifier, pageSize, keySize, valueSize ),
                                      fs, pages, path, pageSize );
    }

    /**
     * Opens an existing store file.
     *
     * @param fs    the file system which holds the store file.
     * @param path  the location in the file system where the store file resides.
     * @param pages the page cache to use for opening the store file.
     * @return the opened store file.
     */
    private KeyValueStoreFile open( FileSystemAbstraction fs, File path, PageCache pages ) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.wrap( new byte[maxSize * 4] );
        try ( StoreChannel file = fs.open( path, "r" ) )
        {
            while ( buffer.hasRemaining() )
            {
                int bytes = file.read( buffer );
                if ( bytes == -1 )
                {
                    break;
                }
            }
        }
        buffer.flip();
        // compute the key sizes
        int keySize = 0;
        while ( buffer.hasRemaining() && buffer.get() == 0 )
        {
            if ( ++keySize > maxSize )
            {
                throw new IOException( "Invalid header, key size too large." );
            }
        }
        // compute the value size
        int valueSize = 1; // start at 1, since we've seen the first non-zero byte
        for ( int zeros = 0; zeros <= keySize; zeros++ )
        {
            if ( !buffer.hasRemaining() )
            {
                throw new IOException( "Invalid value size: " + valueSize );
            }
            if ( buffer.get() != 0 )
            {
                zeros = 0;
            }
            if ( ++valueSize - keySize > maxSize )
            {
                throw new IOException( "Invalid header, value size too large." );
            }
        }
        valueSize -= keySize; // we read in the next zero-key
        // compute a page size that aligns with the <key,value>-tuple size
        int pageSize = pageSize( pages, keySize, valueSize );
        // read the store metadata
        {
            BigEndianByteArrayBuffer formatSpecifier = new BigEndianByteArrayBuffer( new byte[valueSize] );
            writeFormatSpecifier( formatSpecifier );

            PagedFile file = pages.map( path, pageSize );
            try
            {
                BigEndianByteArrayBuffer key = new BigEndianByteArrayBuffer( new byte[keySize] );
                BigEndianByteArrayBuffer value = new BigEndianByteArrayBuffer( new byte[valueSize] );
                // the first value is the format identifier, pass it along
                buffer.position( keySize );
                buffer.limit( keySize + valueSize );
                value.dataFrom( buffer );


                MetadataCollector metadata = metadata( formatSpecifier, pageSize, keySize, valueSize );
                // scan and catalogue all entries in the file
                KeyValueStoreFile.scanAll( file, 0, metadata, key, value );
                KeyValueStoreFile storeFile = new KeyValueStoreFile( file, keySize, valueSize, metadata );
                file = null;
                return storeFile;
            }
            finally
            {
                if ( file != null )
                {
                    file.close();
                }
            }
        }
    }

    private static int pageSize( PageCache pages, int keySize, int valueSize )
    {
        int pageSize = pages == null ? 8192 : pages.pageSize();
        pageSize -= pageSize % (keySize + valueSize);
        return pageSize;
    }

    static boolean validFormatSpecifier( byte[] buffer, int keySize )
    {
        for ( int i = 0, key = 0; i < buffer.length; i++ )
        {
            if ( buffer[i] == 0 )
            {
                if ( i == 0 || ++key == keySize || i == buffer.length - 1 )
                {
                    return false;
                }
            }
            else
            {
                key = 0;
            }
        }
        return true;
    }
}
