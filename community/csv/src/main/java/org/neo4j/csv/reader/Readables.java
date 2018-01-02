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
package org.neo4j.csv.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.neo4j.collection.RawIterator;
import org.neo4j.function.IOFunction;
import org.neo4j.function.IOFunctions;
import org.neo4j.function.ThrowingFunction;

/**
 * Means of instantiating common {@link CharReadable} instances.
 *
 * There are support for compressed files as well for those methods accepting a {@link File} argument.
 * <ol>
 * <li>ZIP: is both an archive and a compression format. In many cases the order of files
 * is important and for a ZIP archive with multiple files, the order of the files are whatever the order
 * set by the tool that created the ZIP archive. Therefore only single-file-zip files are supported.
 * The single file in the given ZIP archive will be decompressed on the fly, while reading.</li>
 * <li>GZIP: is only a compression format and so will be decompressed on the fly, while reading.</li>
 * </ol>
 */
public class Readables
{
    private Readables()
    {
        throw new AssertionError( "No instances allowed" );
    }

    public static final CharReadable EMPTY = new CharReadable.Adapter()
    {
        @Override
        public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
        {
            return buffer;
        }

        @Override
        public void close() throws IOException
        {   // Nothing to close
        }

        @Override
        public String sourceDescription()
        {
            return "EMPTY";
        }
    };

    public static CharReadable wrap( final InputStream stream, final String sourceName, Charset charset )
            throws IOException
    {
        byte[] bytes = new byte[Magic.longest()];
        PushbackInputStream pushbackStream = new PushbackInputStream( stream, bytes.length );
        Charset usedCharset = charset;
        int read = stream.read( bytes );
        if ( read >= 0 )
        {
            bytes = read < bytes.length ? Arrays.copyOf( bytes, read ) : bytes;
            Magic magic = Magic.of( bytes );
            int excessiveBytes = read;
            if ( magic.impliesEncoding() )
            {
                // Unread the diff between the BOM and the longest magic we gathered bytes for
                excessiveBytes -= magic.length();
                usedCharset = magic.encoding();
            }
            pushbackStream.unread( bytes, read - excessiveBytes, excessiveBytes );
        }
        return wrap( new InputStreamReader( pushbackStream, usedCharset )
        {
            @Override
            public String toString()
            {
                return sourceName;
            }
        } );
    }

    /**
     * Remember that the {@link Reader#toString()} must provide a description of the data source.
     */
    public static CharReadable wrap( final Reader reader )
    {
        return new CharReadable.Adapter()
        {
            private long position;
            private final String sourceDescription = reader.toString();

            @Override
            public SectionedCharBuffer read( SectionedCharBuffer buffer, int from ) throws IOException
            {
                buffer.compact( buffer, from );
                buffer.readFrom( reader );
                position += buffer.available();
                return buffer;
            }

            @Override
            public void close() throws IOException
            {
                reader.close();
            }

            @Override
            public long position()
            {
                return position;
            }

            @Override
            public String sourceDescription()
            {
                return sourceDescription;
            }
        };
    }

    private static class FromFile implements IOFunction<File, Reader>
    {
        private final Charset charset;

        FromFile( Charset charset )
        {
            this.charset = charset;
        }

        @Override
        public Reader apply( final File file ) throws IOException
        {
            Magic magic = Magic.of( file );
            if ( magic == Magic.ZIP )
            {   // ZIP file
                ZipFile zipFile = new ZipFile( file );
                ZipEntry entry = getSingleSuitableEntry( zipFile );
                return new InputStreamReader( zipFile.getInputStream( entry ), charset )
                {
                    @Override
                    public String toString()
                    {
                        return file.getPath();
                    }
                };
            }
            else if ( magic == Magic.GZIP )
            {   // GZIP file. GZIP isn't an archive like ZIP, so this is purely data that is compressed.
                // Although a very common way of compressing with GZIP is to use TAR which can combine many
                // files into one blob, which is then compressed. If that's the case then
                // the data will look like garbage and the reader will fail for whatever it will be used for.
                // TODO add tar support
                GZIPInputStream zipStream = new GZIPInputStream( new FileInputStream( file ) );
                return new InputStreamReader( zipStream, charset )
                {
                    @Override
                    public String toString()
                    {
                        return file.getPath();
                    }
                };
            }
            else
            {
                InputStream in = new FileInputStream( file );
                Charset usedCharset = this.charset;
                if ( magic.impliesEncoding() )
                {
                    // Read (and skip) the magic (BOM in this case) from the file we're returning out
                    in.skip( magic.length() );
                    usedCharset = magic.encoding();
                }
                return new InputStreamReader( in, usedCharset )
                {
                    @Override
                    public String toString()
                    {
                        return file.getPath();
                    }
                };
            }
        }

        private ZipEntry getSingleSuitableEntry( ZipFile zipFile ) throws IOException
        {
            List<String> unsuitableEntries = new ArrayList<>();
            Enumeration<? extends ZipEntry> enumeration = zipFile.entries();
            ZipEntry found = null;
            while ( enumeration.hasMoreElements() )
            {
                ZipEntry entry = enumeration.nextElement();
                if ( entry.isDirectory() || invalidZipEntry( entry.getName() ) )
                {
                    unsuitableEntries.add( entry.getName() );
                    continue;
                }

                if ( found != null )
                {
                    throw new IOException( "Multiple suitable files found in zip file " + zipFile.getName() +
                            ", at least " + found.getName() + " and " + entry.getName() +
                            ". Only a single file per zip file is supported" );
                }
                found = entry;
            }

            if ( found == null )
            {
                throw new IOException( "No suitable file found in zip file " + zipFile.getName() + "." +
                        (!unsuitableEntries.isEmpty() ?
                                " Although found these unsuitable entries " + unsuitableEntries : "" ) );
            }
            return found;
        }
    }

    private static boolean invalidZipEntry( String name )
    {
        return name.contains( "__MACOSX" ) ||
               name.startsWith( "." ) ||
               name.contains( "/." );
    }

    public static CharReadable files( Charset charset, File... files ) throws IOException
    {
        IOFunction<File,Reader> opener = new FromFile( charset );
        switch ( files.length )
        {
        case 0:  return EMPTY;
        case 1:  return wrap( opener.apply( files[0] ) );
        default: return new MultiReadable( iterator( files, opener ) );
        }
    }

    public static CharReadable sources( Reader... sources ) throws IOException
    {
        return new MultiReadable( iterator( sources, IOFunctions.<Reader>identity() ) );
    }

    public static CharReadable sources( RawIterator<Reader,IOException> sources ) throws IOException
    {
        return new MultiReadable( sources );
    }

    private static <IN,OUT> RawIterator<OUT,IOException> iterator( final IN[] items,
            final ThrowingFunction<IN,OUT,IOException> converter )
    {
        if ( items.length == 0 )
        {
            throw new IllegalStateException( "No source items specified" );
        }

        return new RawIterator<OUT,IOException>()
        {
            private int cursor;

            @Override
            public boolean hasNext()
            {
                return cursor < items.length;
            }

            @Override
            public OUT next() throws IOException
            {
                if ( !hasNext() )
                {
                    throw new IllegalStateException();
                }
                return converter.apply( items[cursor++] );
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
