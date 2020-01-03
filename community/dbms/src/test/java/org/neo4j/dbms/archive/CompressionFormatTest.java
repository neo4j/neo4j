/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.dbms.archive;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.dbms.archive.CompressionFormat.GZIP;
import static org.neo4j.dbms.archive.CompressionFormat.ZSTD;
import static org.neo4j.dbms.archive.CompressionFormat.selectCompressionFormat;
import static org.neo4j.test.proc.ProcessUtil.getClassPath;
import static org.neo4j.test.proc.ProcessUtil.getJavaExecutable;

class CompressionFormatTest
{
    @Test
    void shouldSelectZstdAsDefault()
    {
        assertEquals( ZSTD, selectCompressionFormat() );
    }

    @Test
    void shouldFallbackToGzipWhenZstdFails() throws Exception
    {
        // this test runs in a separate process to avoid problems with any parallel execution and shared static states
        int expectedExitCode = 66;
        String[] args = {
                getJavaExecutable().toString(),
                "-cp", getClassPath(),
                CompressionFormatTest.class.getName(),
                Integer.toString( expectedExitCode ) // using exitcode to verify execution of correct function
        };
        Process start = new ProcessBuilder( args )
                .inheritIO() // dont hide any errors
                .start();
        assertEquals( expectedExitCode, start.waitFor() );

    }

    public static void main( String[] args )
    {
        int exitCode = Integer.parseInt( args[0] );
        System.setProperty( "os.arch", "foo" ); // sabotage ZSTD loading
        CompressionFormat format = selectCompressionFormat();
        assertEquals( GZIP, format, String.format( "Should fallback to %s when %s fails", GZIP.name(), ZSTD.name() ) );
        System.exit( exitCode );
    }
}
