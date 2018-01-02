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
package org.neo4j.kernel.api.impl.index;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import org.neo4j.io.fs.FileUtils.FileOperation;

import static org.neo4j.io.fs.FileUtils.windowsSafeIOOperation;

public class DirectorySupport
{
    public static void deleteDirectoryContents( Directory directory ) throws IOException
    {
        for ( final String fileName : directory.listAll() )
        {
            windowsSafeDelete( directory, fileName );
        }
    }

    public static void archiveAndDeleteDirectoryContents( Directory directory, String archiveName ) throws IOException
    {
        // NOTE: since the archive is created in the same directory as the files to be put in the archive are taken
        // from, repeated archiving of the same directory will nest archives within archives.
        String[] files = directory.listAll();
        // we must list the files before we create the output, to avoid listing the output file
        if ( files.length > 0 )
        {
            byte[] buffer = new byte[4 * 1024];
            try ( ZipOutputStream zip = new ZipOutputStream( new IndexOutputStream( directory, archiveName ) ) )
            {
                for ( final String fileName : files )
                {
                    zip.putNextEntry( new ZipEntry( fileName ) );
                    try ( IndexInput input = directory.openInput( fileName ) )
                    {
                        for ( long pos = 0, size = input.length(); pos < size; )
                        {
                            int read = Math.min( buffer.length, (int) (size - pos) );
                            input.readBytes( buffer, 0, read );
                            pos += read;
                            zip.write( buffer, 0, read );
                        }
                    }
                    zip.closeEntry();
                    windowsSafeDelete( directory, fileName );
                }
            }
        }
    }

    private static void windowsSafeDelete( final Directory directory, final String fileName ) throws IOException
    {
        windowsSafeIOOperation( new FileOperation()
        {
            @Override
            public void perform() throws IOException
            {
                directory.deleteFile( fileName );
            }
        } );
    }

    private static class IndexOutputStream extends OutputStream
    {
        private final IndexOutput output;

        IndexOutputStream( Directory directory, String name ) throws IOException
        {
            this.output = directory.createOutput( name );
        }

        @Override
        public void write( int b ) throws IOException
        {
            output.writeByte( (byte) (0xFF & b) );
        }

        @Override
        public void write( byte[] b, int off, int len ) throws IOException
        {
            output.writeBytes( b, off, len );
        }

        @Override
        public void flush() throws IOException
        {
            output.flush();
        }

        @Override
        public void close() throws IOException
        {
            output.close();
        }
    }
}
