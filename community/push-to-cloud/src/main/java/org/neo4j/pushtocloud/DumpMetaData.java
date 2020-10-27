/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.pushtocloud;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.neo4j.dbms.archive.CompressionFormat.ZSTD;

public class DumpMetaData
{
    public final String format;
    public final String fileCount;
    public final String byteCount;

    private DumpMetaData( String format, String fileCount, String byteCount )
    {
        this.format = format;
        this.fileCount = fileCount;
        this.byteCount = byteCount;
    }

    /**
     * This method is based on the Neo4j 4.0 code for reading meta-data. See the Loader.java file in Neo4j 4.0 and associated support in CompressionFormat. It
     * is designed to only work with dump files made with newer Neo4j 3.x, and older GZIP files will throw exceptions and fail the push-to-cloud command.
     */
    public static DumpMetaData getMetaData( Path archive ) throws IOException
    {
        try ( InputStream input = Files.newInputStream( archive ) )
        {
            // Important: Only the ZSTD compressed archives have any archive metadata.
            InputStream decompressor = ZSTD.decompress( input );    // throws exception if not ZSTD
            DataInputStream metadata = new DataInputStream( decompressor );
            int version = metadata.readInt();
            if ( version == 1 )
            {
                String format = "Neo4j ZSTD Dump.";
                String fileCount = String.valueOf( metadata.readLong() );
                String byteCount = String.valueOf( metadata.readLong() );
                return new DumpMetaData( format, fileCount, byteCount );
            }
            else
            {
                throw new IOException( "Cannot read archive meta-data. I don't recognise this archive version: " + version + "." );
            }
        }
    }
}
