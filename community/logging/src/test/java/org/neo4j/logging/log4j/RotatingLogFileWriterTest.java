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
package org.neo4j.logging.log4j;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.assertj.core.api.Assertions.assertThat;

@TestDirectoryExtension
class RotatingLogFileWriterTest
{
    @Inject
    private DefaultFileSystemAbstraction fs;
    @Inject
    private TestDirectory dir;

    private RotatingLogFileWriter writer;

    @AfterEach
    void tearDown() throws IOException
    {
        writer.close();
    }

    @Test
    void shouldRotateOnThreshold()
    {
        Path targetFile = dir.homePath().resolve( "test.log" );
        Path targetFile1 = dir.homePath().resolve( "test.log.1" );
        Path targetFile2 = dir.homePath().resolve( "test.log.2" );

        writer = new RotatingLogFileWriter( fs, targetFile, 10, 2, "myHeader%n" );

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );

        writer.printf( "more than 10B that will trigger rotation on next written message" );

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile1 ) ).isEqualTo( false );

        writer.printf( "test string" );
        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile1 ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile2 ) ).isEqualTo( false );
    }

    @Test
    void rotationShouldRespectMaxArchives() throws IOException
    {
        Path targetFile = dir.homePath().resolve( "test.log" );
        Path targetFile1 = dir.homePath().resolve( "test.log.1" );
        Path targetFile2 = dir.homePath().resolve( "test.log.2" );
        Path targetFile3 = dir.homePath().resolve( "test.log.3" );

        writer = new RotatingLogFileWriter( fs, targetFile, 10, 2, "" );

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );

        writer.printf( "test string 1" );
        writer.printf( "test string 2" );
        writer.printf( "test string 3" );
        writer.printf( "test string 4" );
        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile1 ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile2 ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile3 ) ).isEqualTo( false );

        assertThat( Files.readAllLines( targetFile ) ).containsSequence( "test string 4" );
        assertThat( Files.readAllLines( targetFile1 ) ).containsSequence( "test string 3" );
        assertThat( Files.readAllLines( targetFile2 ) ).containsSequence( "test string 2" );
    }

    @Test
    void headerShouldBeUsedInEachFile() throws IOException
    {
        Path targetFile = dir.homePath().resolve( "test.log" );
        Path targetFile1 = dir.homePath().resolve( "test.log.1" );

        writer = new RotatingLogFileWriter( fs, targetFile, 10, 2, "my header%n" );

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );

        writer.printf( "Long line that will get next message to be written to next file" );
        writer.printf( "test2" );

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );
        assertThat( fs.fileExists( targetFile1 ) ).isEqualTo( true );

        assertThat( Files.readAllLines( targetFile1 ) ).containsSequence( "my header", "Long line that will get next message to be written to next file" );
        assertThat( Files.readAllLines( targetFile ) ).containsSequence( "my header", "test2" );
    }

    @Test
    void shouldHandleFormatStrings() throws IOException
    {
        Path targetFile = dir.homePath().resolve( "test.log" );

        writer = new RotatingLogFileWriter( fs, targetFile, 100, 2, "" );

        assertThat( fs.fileExists( targetFile ) ).isEqualTo( true );

        writer.printf( "%s,%d,%f", "string", 1, 1.234567f );
        writer.printf( "test2" );

        assertThat( Files.readAllLines( targetFile ) ).containsSequence( "string,1,1.234567", "test2" );
    }
}
