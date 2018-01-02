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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;

class KeyValueWriter implements Closeable
{
    private final MetadataCollector metadata;
    private final Writer writer;
    private int keySize, valueSize;
    private State state = State.expecting_format_specifier;

    public static KeyValueWriter create(
            MetadataCollector metadata, FileSystemAbstraction fs, PageCache pages, File path, int pageSize )
            throws IOException
    {
        return new KeyValueWriter( metadata, Writer.create( fs, pages, path, pageSize ) );
    }

    KeyValueWriter( MetadataCollector metadata, Writer writer )
    {
        this.metadata = metadata;
        this.writer = writer;
    }

    public boolean writeHeader( BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value ) throws IOException
    {
        boolean result = state.header( this, value.allZeroes() || value.minusOneAtTheEnd() );
        doWrite( key, value, State.done );
        return result;
    }

    public void writeData( BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value ) throws IOException
    {
        state.data( this );
        assert key.size() == keySize;
        assert value.size() == valueSize;
        if ( key.allZeroes() )
        {
            state = State.in_error;
            throw new IllegalArgumentException( "All-zero keys are not allowed." );
        }
        if ( !write( key, value ) )
        {
            state = State.in_error;
            throw new IllegalStateException( "MetadataCollector stopped on data field." );
        }
    }

    private void doWrite( BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value, State expectedNextState )
            throws IOException
    {
        this.keySize = key.size();
        this.valueSize = value.size();
        assert key.allZeroes() : "key should have been cleared by previous call";
        if ( !write( key, value ) )
        {
            if ( state != expectedNextState )
            {
                state = State.in_error;
                throw new IllegalStateException(
                        "MetadataCollector stopped before " + expectedNextState + " reached." );
            }
        }
    }

    private boolean write( BigEndianByteArrayBuffer key, BigEndianByteArrayBuffer value ) throws IOException
    {
        boolean result = metadata.visit( key, value );
        writer.write( key.buffer );
        writer.write( value.buffer );
        key.clear();
        value.clear();
        return result;
    }

    public KeyValueStoreFile openStoreFile() throws IOException
    {
        state.open( this );
        return writer.open( metadata, keySize, valueSize );
    }

    @Override
    public void close() throws IOException
    {
        writer.close();
    }

    private enum State
    {   // <pre>
        expecting_format_specifier
        {
            @Override
            boolean header( KeyValueWriter writer, boolean zeroValueOrMinusOne )
            {
                if ( zeroValueOrMinusOne )
                {
                    writer.state = in_error;
                    return false;
                }
                else
                {
                    writer.state = expecting_header;
                    return true;
                }
            }
        },
        expecting_header
        {
            @Override
            boolean header( KeyValueWriter writer, boolean zeroValueOrMinusOne )
            {
                writer.state = zeroValueOrMinusOne ? expecting_data : writing_header;
                return true;
            }
        },
        writing_header
        {
            @Override
            boolean header( KeyValueWriter writer, boolean zeroValueOrMinusOne )
            {
                if ( zeroValueOrMinusOne )
                {
                    writer.state = done;
                }
                return true;
            }

            @Override
            void data( KeyValueWriter writer )
            {
                writer.state = writing_data;
            }
        },
        expecting_data
        {
            @Override
            boolean header( KeyValueWriter writer, boolean zeroValueOrMinusOne )
            {
                if ( zeroValueOrMinusOne )
                {
                    writer.state = done;
                    return true;
                }
                else
                {
                    writer.state = in_error;
                    return false;
                }
            }

            @Override
            void data( KeyValueWriter writer )
            {
                writer.state = writing_data;
            }
        },
        writing_data
        {
            @Override
            boolean header( KeyValueWriter writer, boolean zeroValueOrMinusOne )
            {
                if ( zeroValueOrMinusOne )
                {
                    writer.state = in_error;
                    return false;
                }
                else
                {
                    writer.state = done;
                    return true;
                }
            }

            @Override
            void data( KeyValueWriter writer )
            {
                // keep the same state
            }
        },
        done
        {
            @Override
            void open( KeyValueWriter writer )
            {
                // ok
            }
        },
        in_error;
        // </pre>

        boolean header( KeyValueWriter writer, boolean zeroValueOrMinusOne )
        {
            throw illegalState( writer, "write header" );
        }

        void data( KeyValueWriter writer )
        {
            throw illegalState( writer, "write data" );
        }

        void open( KeyValueWriter writer )
        {
            throw illegalState( writer, "open store file" );
        }

        private IllegalStateException illegalState( KeyValueWriter writer, String what )
        {
            writer.state = in_error;
            return new IllegalStateException( "Cannot " + what + " when " + name().replace( '_', ' ' ) + "." );
        }
    }

    static abstract class Writer
    {
        private static final boolean WRITE_TO_PAGE_CACHE =
                flag( KeyValueWriter.class, "WRITE_TO_PAGE_CACHE", false );

        abstract void write( byte[] data ) throws IOException;

        abstract KeyValueStoreFile open( Metadata metadata, int keySize, int valueSize ) throws IOException;

        abstract void close() throws IOException;

        static Writer create( FileSystemAbstraction fs, PageCache pages, File path, int pageSize ) throws IOException
        {
            if ( pages == null )
            {
                return new StreamWriter( fs.openAsOutputStream( path, false ) );
            }
            else if ( WRITE_TO_PAGE_CACHE )
            {
                return new PageWriter( pages.map( path, pageSize ) );
            }
            else
            {
                return new OpeningStreamWriter( fs.openAsOutputStream( path, false ), pages, path, pageSize );
            }
        }
    }

    private static class StreamWriter extends Writer
    {
        private final OutputStream out;

        StreamWriter( OutputStream out )
        {
            this.out = out;
        }

        @Override
        void write( byte[] data ) throws IOException
        {
            out.write( data );
        }

        @Override
        KeyValueStoreFile open( Metadata metadata, int keySize, int valueSize ) throws IOException
        {
            return null;
        }

        @Override
        void close() throws IOException
        {
            out.flush();
            out.close();
        }
    }

    private static class OpeningStreamWriter extends StreamWriter
    {
        private final PageCache pages;
        private final File path;
        private final int pageSize;

        OpeningStreamWriter( OutputStream out, PageCache pages, File path, int pageSize )
        {
            super( out );
            this.pages = pages;
            this.path = path;
            this.pageSize = pageSize;
        }

        @Override
        KeyValueStoreFile open( Metadata metadata, int keySize, int valueSize ) throws IOException
        {
            return new KeyValueStoreFile( pages.map( path, pageSize ), keySize, valueSize, metadata );
        }
    }

    private static class PageWriter extends Writer
    {
        private final PagedFile file;
        private PageCursor cursor;
        private boolean opened;

        PageWriter( PagedFile file ) throws IOException
        {
            this.file = file;
            this.cursor = file.io( 0, PagedFile.PF_EXCLUSIVE_LOCK );
            cursor.next();
        }

        @Override
        void write( byte[] data ) throws IOException
        {
            if ( cursor.getOffset() == file.pageSize() )
            {
                cursor.next();
            }
            cursor.putBytes( data );
        }

        @Override
        KeyValueStoreFile open( Metadata metadata, int keySize, int valueSize ) throws IOException
        {
            KeyValueStoreFile result = new KeyValueStoreFile( file, keySize, valueSize, metadata );
            opened = true;
            return result;
        }

        @Override
        void close() throws IOException
        {
            cursor.close();
            cursor = null;
            if ( opened )
            {
                file.flushAndForce();
            }
            else
            {
                file.close();
            }
        }
    }
}
