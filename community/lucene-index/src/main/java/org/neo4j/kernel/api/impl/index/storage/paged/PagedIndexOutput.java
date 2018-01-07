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
package org.neo4j.kernel.api.impl.index.storage.paged;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.OutputStreamIndexOutput;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * This is based on {@link FSDirectory.FSIndexOutput}, only change is that it works via {@link FileSystemAbstraction}.
 */
class PagedIndexOutput extends OutputStreamIndexOutput
{
    /**
     * The maximum chunk size is 8192 bytes, because file channel mallocs
     * a native buffer outside of stack if the write buffer size is larger.
     *
     * See http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/b1fec09a9588/src/share/native/java/io/io_util.c and
     * https://issues.apache.org/jira/browse/LUCENE-5164
     */
    private static final int CHUNK_SIZE = 8192;

    PagedIndexOutput( Path path, FileSystemAbstraction fs ) throws IOException
    {
        super( "PagedIndexOutput(path=\"" + path + "\")",
                new FilterOutputStream( fs.openAsOutputStream( path.toFile(), false ) )
                {
                    // Ensure never write more than CHUNK_SIZE bytes:
                    @Override
                    public void write( byte[] b, int offset, int length ) throws IOException
                    {
                        while ( length > 0 )
                        {
                            final int chunk = Math.min( length, CHUNK_SIZE );
                            out.write( b, offset, chunk );
                            length -= chunk;
                            offset += chunk;
                        }
                    }
                }, CHUNK_SIZE );
    }
}
