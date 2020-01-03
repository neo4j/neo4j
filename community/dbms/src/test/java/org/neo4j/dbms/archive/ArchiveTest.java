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

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.function.Predicates;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.file.Files.isDirectory;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.helpers.collection.Pair.pair;

@ExtendWith( TestDirectoryExtension.class )
class ArchiveTest
{
    @Inject
    private TestDirectory testDirectory;

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripAnEmptyDirectory( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Files.createDirectories( directory );

        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripASingleFile( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Files.createDirectories( directory );
        Files.write( directory.resolve( "a-file" ), "text".getBytes() );

        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripAnEmptyFile( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Files.createDirectories( directory );
        Files.write( directory.resolve( "a-file" ), new byte[0] );

        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripFilesWithDifferentContent( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Files.createDirectories( directory );
        Files.write( directory.resolve( "a-file" ), "text".getBytes() );
        Files.write( directory.resolve( "another-file" ), "some-different-text".getBytes() );

        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripEmptyDirectories( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path subdir = directory.resolve( "a-subdirectory" );
        Files.createDirectories( subdir );
        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldRoundTripFilesInDirectories( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path subdir = directory.resolve( "a-subdirectory" );
        Files.createDirectories( subdir );
        Files.write( subdir.resolve( "a-file" ), "text".getBytes() );
        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldCopeWithLongPaths( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path subdir = directory.resolve( "a/very/long/path/which/is/not/realistic/for/a/database/today/but/which" +
                "/ensures/that/we/dont/get/caught/out/at/in/the/future/the/point/being/that/there/are/multiple/tar" +
                "/formats/some/of/which/do/not/cope/with/long/paths" );
        Files.createDirectories( subdir );
        Files.write( subdir.resolve( "a-file" ), "text".getBytes() );
        assertRoundTrips( directory, compressionFormat );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldExcludeFilesMatchedByTheExclusionPredicate( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Files.createDirectories( directory );
        Files.write( directory.resolve( "a-file" ), new byte[0] );
        Files.write( directory.resolve( "another-file" ), new byte[0] );

        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        new Dumper().dump( directory, directory, archive, compressionFormat, path -> path.getFileName().toString().equals( "another-file" ) );
        Path newDirectory = testDirectory.file( "the-new-directory" ).toPath();
        new Loader().load( archive, newDirectory, newDirectory );

        Path expectedOutput = testDirectory.directory( "expected-output" ).toPath();
        Files.createDirectories( expectedOutput );
        Files.write( expectedOutput.resolve( "a-file" ), new byte[0] );

        assertEquals( describeRecursively( expectedOutput ), describeRecursively( newDirectory ) );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void shouldExcludeWholeDirectoriesMatchedByTheExclusionPredicate( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "a-directory" ).toPath();
        Path subdir = directory.resolve( "subdir" );
        Files.createDirectories( subdir );
        Files.write( subdir.resolve( "a-file" ), new byte[0] );

        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        new Dumper().dump( directory, directory, archive, compressionFormat, path -> path.getFileName().toString().equals( "subdir" ) );
        Path newDirectory = testDirectory.file( "the-new-directory" ).toPath();
        new Loader().load( archive, newDirectory, newDirectory );

        Path expectedOutput = testDirectory.directory( "expected-output" ).toPath();
        Files.createDirectories( expectedOutput );

        assertEquals( describeRecursively( expectedOutput ), describeRecursively( newDirectory ) );
    }

    @ParameterizedTest
    @EnumSource( CompressionFormat.class )
    void dumpAndLoadTransactionLogsFromCustomLocations( CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path directory = testDirectory.directory( "dbDirectory" ).toPath();
        Path txLogsDirectory = testDirectory.directory( "txLogsDirectory" ).toPath();
        Files.write( directory.resolve( "dbfile" ), new byte[0] );
        Files.write( txLogsDirectory.resolve( TransactionLogFiles.DEFAULT_NAME + ".0" ), new byte[0] );

        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        new Dumper().dump( directory, txLogsDirectory, archive, compressionFormat, Predicates.alwaysFalse() );
        Path newDirectory = testDirectory.file( "the-new-directory" ).toPath();
        Path newTxLogsDirectory = testDirectory.file( "newTxLogsDirectory" ).toPath();
        new Loader().load( archive, newDirectory, newTxLogsDirectory );

        Path expectedOutput = testDirectory.directory( "expected-output" ).toPath();
        Files.createDirectories( expectedOutput );
        Files.write( expectedOutput.resolve( "dbfile" ), new byte[0] );

        Path expectedTxLogs = testDirectory.directory( "expectedTxLogs" ).toPath();
        Files.createDirectories( expectedTxLogs );
        Files.write( expectedTxLogs.resolve( TransactionLogFiles.DEFAULT_NAME + ".0" ), new byte[0] );

        assertEquals( describeRecursively( expectedOutput ), describeRecursively( newDirectory ) );
        assertEquals( describeRecursively( expectedTxLogs ), describeRecursively( newTxLogsDirectory ) );
    }

    private void assertRoundTrips( Path oldDirectory, CompressionFormat compressionFormat ) throws IOException, IncorrectFormat
    {
        Path archive = testDirectory.file( "the-archive.dump" ).toPath();
        new Dumper().dump( oldDirectory, oldDirectory, archive, compressionFormat, Predicates.alwaysFalse() );
        Path newDirectory = testDirectory.file( "the-new-directory" ).toPath();
        new Loader().load( archive, newDirectory, newDirectory );

        assertEquals( describeRecursively( oldDirectory ), describeRecursively( newDirectory ) );
    }

    private Map<Path,Description> describeRecursively( Path directory ) throws IOException
    {
        return Files.walk( directory ).map( path -> pair( directory.relativize( path ), describe( path ) ) ).collect( HashMap::new,
                ( pathDescriptionHashMap, pathDescriptionPair ) -> pathDescriptionHashMap.put( pathDescriptionPair.first(), pathDescriptionPair.other() ),
                HashMap::putAll );
    }

    private Description describe( Path file )
    {
        try
        {
            return isDirectory( file ) ? new DirectoryDescription() : new FileDescription( Files.readAllBytes( file ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private interface Description
    {
    }

    private class DirectoryDescription implements Description
    {
        @Override
        public boolean equals( Object o )
        {
            return this == o || !(o == null || getClass() != o.getClass());
        }

        @Override
        public int hashCode()
        {
            return 1;
        }
    }

    private class FileDescription implements Description
    {
        private final byte[] bytes;

        FileDescription( byte[] bytes )
        {
            this.bytes = bytes;
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
            FileDescription that = (FileDescription) o;
            return Arrays.equals( bytes, that.bytes );
        }

        @Override
        public int hashCode()
        {
            return Arrays.hashCode( bytes );
        }
    }
}
