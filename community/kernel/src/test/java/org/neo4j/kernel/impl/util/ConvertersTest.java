/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import org.neo4j.test.rule.TestDirectory;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.util.Converters.regexFiles;
import static org.neo4j.kernel.impl.util.Converters.toOptionalHostnamePortFromRawAddress;

public class ConvertersTest
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    @Test
    public void shouldSortFilesByNumberCleverly() throws Exception
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
    public void canProcessPortFromAGivenString()
    {
        // given
        String addressWithPorts = "hostname:1234";

        // when
        Optional<Integer> port = toOptionalHostnamePortFromRawAddress( addressWithPorts ).getPort();

        // then
        assertTrue( port.isPresent() );
        assertEquals( new Integer( 1234 ), port.get() );
    }

    @Test
    public void emptyOptionalWhenPortIsMissing()
    {
        //given
        String addressWithoutPorts = "hostname";

        // when
        Optional<Integer> port = toOptionalHostnamePortFromRawAddress( addressWithoutPorts ).getPort();

        // then
        assertFalse( port.isPresent() );
    }

    @Test
    public void canProcessHostnameFromAGivenAddress()
    {
        // given
        String addressWithPorts = "hostname:1234";

        // when
        Optional<String> hostname = toOptionalHostnamePortFromRawAddress( addressWithPorts ).getHostname();

        // then
        assertTrue( hostname.isPresent() );
        assertEquals( "hostname", hostname.get() );
    }

    @Test
    public void canProcessHostnameWithoutPort()
    {
        // given
        String addressWithoutPort = "hostname";

        // when
        Optional<String> hostname = toOptionalHostnamePortFromRawAddress( addressWithoutPort ).getHostname();

        // then
        assertTrue( hostname.isPresent() );
        assertEquals( "hostname", hostname.get() );
    }

    @Test
    public void emptyOptionalWhenOnlyPort()
    {
        // given
        String portOnlyAddress = ":1234";

        // when
        Optional<String> hostname = toOptionalHostnamePortFromRawAddress( portOnlyAddress ).getHostname();

        // then
        assertFalse( hostname.isPresent() );
    }

    private File existenceOfFile( String name ) throws IOException
    {
        File file = directory.file( name );
        file.createNewFile();
        return file;
    }
}
