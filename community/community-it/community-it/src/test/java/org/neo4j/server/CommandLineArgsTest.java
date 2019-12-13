/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.server;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;

import org.neo4j.configuration.Config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.internal.helpers.ArrayUtil.array;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;

public class CommandLineArgsTest
{
    @Test
    void shouldPickUpSpecifiedConfigFile()
    {
        File dir = new File( "/some-dir" ).getAbsoluteFile();
        Optional<File> expectedFile = Optional.of( new File( dir, Config.DEFAULT_CONFIG_FILE_NAME ) );
        assertEquals( expectedFile, parse( "--config-dir", dir.toString() ).configFile() );
        assertEquals( expectedFile, parse( "--config-dir=" + dir ).configFile() );
    }

    @Test
    void shouldResolveConfigFileRelativeToWorkingDirectory()
    {
        Optional<File> expectedFile = Optional.of( new File( "some-dir", Config.DEFAULT_CONFIG_FILE_NAME ) );
        assertEquals( expectedFile, parse( "--config-dir", "some-dir" ).configFile() );
        assertEquals( expectedFile, parse( "--config-dir=some-dir" ).configFile() );
    }

    @Test
    void shouldReturnNullIfConfigDirIsNotSpecified()
    {
        assertEquals( Optional.empty(), parse().configFile() );
    }

    @Test
    void shouldPickUpSpecifiedHomeDir()
    {
        File homeDir = new File( "/some/absolute/homedir" ).getAbsoluteFile();

        assertEquals( homeDir, parse( "--home-dir", homeDir.toString() ).homeDir() );
        assertEquals( homeDir, parse( "--home-dir=" + homeDir.toString() ).homeDir() );
    }

    @Test
    void shouldReturnNullIfHomeDirIsNotSpecified()
    {
        assertNull( parse().homeDir() );
    }

    @Test
    void shouldPickUpOverriddenConfigurationParameters()
    {
        // GIVEN
        String[] args = array( "-c", "myoption=myvalue" );

        // WHEN
        CommandLineArgs parsed = CommandLineArgs.parse( args );

        // THEN
        assertEquals( stringMap( "myoption", "myvalue" ),
                parsed.configOverrides() );
    }

    @Test
    void shouldPickUpOverriddenBooleanConfigurationParameters()
    {
        // GIVEN
        String[] args = array( "-c", "myoptionenabled" );

        // WHEN
        CommandLineArgs parsed = CommandLineArgs.parse( args );

        // THEN
        assertEquals( stringMap( "myoptionenabled", Boolean.TRUE.toString() ),
                parsed.configOverrides()  );
    }

    @Test
    void shouldPickUpMultipleOverriddenConfigurationParameters()
    {
        // GIVEN
        String[] args = array(
                "-c", "my_first_option=first",
                "-c", "myoptionenabled",
                "-c", "my_second_option=second" );

        // WHEN
        CommandLineArgs parsed = CommandLineArgs.parse( args );

        // THEN
        assertEquals( stringMap( "my_first_option", "first" ,
                        "myoptionenabled", Boolean.TRUE.toString(),
                       "my_second_option", "second" ),
                parsed.configOverrides() );
    }

    private CommandLineArgs parse( String... args )
    {
        return CommandLineArgs.parse( args );
    }
}
