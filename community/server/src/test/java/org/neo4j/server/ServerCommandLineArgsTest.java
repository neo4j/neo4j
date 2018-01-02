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
package org.neo4j.server;

import org.junit.Test;

import java.io.File;

import org.neo4j.helpers.Pair;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class ServerCommandLineArgsTest
{
    @Test
    public void shouldPickUpSpecifiedConfigFile() throws Exception
    {
        shouldPickUpSpecifiedConfigFile( ServerCommandLineArgs.CONFIG_KEY_ALT_1 );
        shouldPickUpSpecifiedConfigFile( ServerCommandLineArgs.CONFIG_KEY_ALT_2 );
    }

    public void shouldPickUpSpecifiedConfigFile( String key )
    {
        // GIVEN
        String customConfigFile = "MyConfigFile";
        String[] args = array( "--" + key, customConfigFile );

        // WHEN
        ServerCommandLineArgs parsed = ServerCommandLineArgs.parse( args );

        // THEN
        assertEquals( new File( customConfigFile ), parsed.configFile() );
    }

    @Test
    public void shouldPickUpOverriddenConfigurationParameters() throws Exception
    {
        // GIVEN
        String[] args = array( "-c", "myoption=myvalue" );

        // WHEN
        ServerCommandLineArgs parsed = ServerCommandLineArgs.parse( args );

        // THEN
        assertEquals( asSet( Pair.of( "myoption", "myvalue" ) ),
                asSet( parsed.configOverrides() ) );
    }

    @Test
    public void shouldPickUpOverriddenBooleanConfigurationParameters() throws Exception
    {
        // GIVEN
        String[] args = array( "-c", "myoptionenabled" );

        // WHEN
        ServerCommandLineArgs parsed = ServerCommandLineArgs.parse( args );

        // THEN
        assertEquals( asSet( Pair.of( "myoptionenabled", Boolean.TRUE.toString() ) ),
                asSet( parsed.configOverrides() ) );
    }

    @Test
    public void shouldPickUpMultipleOverriddenConfigurationParameters() throws Exception
    {
        // GIVEN
        String[] args = array(
                "-c", "my_first_option=first",
                "-c", "myoptionenabled",
                "-c", "my_second_option=second" );

        // WHEN
        ServerCommandLineArgs parsed = ServerCommandLineArgs.parse( args );

        // THEN
        assertEquals( asSet(
                Pair.of( "my_first_option", "first" ),
                Pair.of( "myoptionenabled", Boolean.TRUE.toString() ),
                Pair.of( "my_second_option", "second" ) ),

                asSet( parsed.configOverrides() ) );
    }
}
