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
package org.neo4j.kernel.impl.util;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.apache.commons.lang3.SystemUtils.IS_OS_WINDOWS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.neo4j.kernel.impl.util.Converters.regexFiles;
import static org.neo4j.kernel.impl.util.Converters.toFiles;

@TestDirectoryExtension
class ConvertersTest
{
    @Inject
    private TestDirectory directory;

    @Test
    void shouldSortFilesByNumberCleverly() throws Exception
    {
        // GIVEN
        File file1 = existenceOfFile( "file1" );
        File file123 = existenceOfFile( "file123" );
        File file12 = existenceOfFile( "file12" );
        File file2 = existenceOfFile( "file2" );
        File file32 = existenceOfFile( "file32" );

        // WHEN
        File[] files = regexFiles( true ).apply( directory.file( "file.*" ).getAbsolutePath() );

        // THEN
        assertArrayEquals( new File[]{file1, file2, file12, file32, file123}, files );
    }

    @Test
    void shouldParseFile() throws IOException
    {
        // given
        File file = existenceOfFile( "file" );

        // when
        File[] files = regexFiles( true ).apply( file.getPath() );

        // then
        assertEquals( List.of( file ), List.of( files ) );
    }

    @Test
    void shouldParseRegexFileWithDashes() throws IOException
    {
        assumeFalse( IS_OS_WINDOWS );
        // given
        File file1 = existenceOfFile( "file_1" );
        File file3 = existenceOfFile( "file_3" );
        File file12 = existenceOfFile( "file_12" );

        // when
        File[] files = regexFiles( true ).apply( file1.getParent() + File.separator + "file_\\d+" );
        File[] files2 = regexFiles( true ).apply( file1.getParent() + File.separator + "file_\\d{1,5}" );

        // then
        assertEquals( List.of( file1, file3, file12 ), List.of( files ) );
        assertEquals( List.of( file1, file3, file12 ), List.of( files2 ) );
    }

    @Test
    void shouldParseRegexFileWithDoubleDashes() throws IOException
    {
        // given
        File file1 = existenceOfFile( "file_1" );
        File file3 = existenceOfFile( "file_3" );
        File file12 = existenceOfFile( "file_12" );

        // when
        File[] files = regexFiles( true ).apply( file1.getParent() + File.separator + "file_\\\\d+" );
        File[] files2 = regexFiles( true ).apply( file1.getParent() + File.separator + "file_\\\\d{1,5}" );

        // then
        assertEquals( List.of( file1, file3, file12 ), List.of( files ) );
        assertEquals( List.of( file1, file3, file12 ), List.of( files2 ) );
    }

    @Test
    void shouldConsiderInnerQuotationWhenSplittingMultipleFiles() throws IOException
    {
        // given
        File header = existenceOfFile( "header.csv" );
        File file1 = existenceOfFile( "file_1.csv" );
        File file3 = existenceOfFile( "file_3.csv" );
        File file12 = existenceOfFile( "file_12.csv" );

        // when
        Function<String,File[]> regexMatcher = regexFiles( true );
        Function<String,File[]> converter = toFiles( ",", regexMatcher );
        File[] files = converter.apply( header.getPath() + ",'" + header.getParent() + File.separator + "file_\\\\d{1,5}.csv'" );

        // then
        assertEquals( List.of( header, file1, file3, file12 ), List.of( files ) );
    }

    @Test
    void shouldFailWithProperErrorMessageOnMissingEndQuote()
    {
        // given
        Function<String,File[]> regexMatcher = s ->
        {
            throw new UnsupportedOperationException( "Should not required" );
        };
        Function<String,File[]> converter = toFiles( ",", regexMatcher );

        // when/then
        IllegalStateException exception = assertThrows( IllegalStateException.class, () -> converter.apply( "thing1,'thing2,test,thing3" ) );
        assertThat( exception.getMessage(), containsString( "no matching end quote" ) );
    }

    private File existenceOfFile( String name ) throws IOException
    {
        File file = directory.file( name );
        assertTrue( file.createNewFile() );
        return file;
    }
}
