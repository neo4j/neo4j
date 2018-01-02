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
package org.neo4j.helpers;

import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.Functions.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.MANDATORY;
import static org.neo4j.kernel.configuration.Settings.NORMALIZED_RELATIVE_URI;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.basePath;
import static org.neo4j.kernel.configuration.Settings.isFile;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.matches;
import static org.neo4j.kernel.configuration.Settings.max;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.range;
import static org.neo4j.kernel.configuration.Settings.setting;

public class SettingsTest
{
    @Test
    public void testInteger()
    {
        Setting<Integer> setting = setting( "foo", INTEGER, "3" );

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "4" ) ) ), equalTo( 4 ) );

        // Bad
        try
        {
            setting.apply( map( stringMap( "foo", "bar" ) ) );
            fail();
        }
        catch ( InvalidSettingException e )
        {
            // Ok
        }
    }

    @Test
    public void testList()
    {
        Setting<List<Integer>> setting = setting( "foo", list( ",", INTEGER ), "1,2,3,4" );
        assertThat( setting.apply( map( stringMap() ) ).toString(), equalTo( "[1, 2, 3, 4]" ) );

        Setting<List<Integer>> setting2 = setting( "foo", list( ",", INTEGER ), "1,2,3,4," );
        assertThat( setting2.apply( map( stringMap() ) ).toString(), equalTo( "[1, 2, 3, 4]" ) );

        Setting<List<Integer>> setting3 = setting( "foo", list( ",", INTEGER ), "" );
        assertThat( setting3.apply( map( stringMap() ) ).toString(), equalTo( "[]" ) );

        Setting<List<Integer>> setting4 = setting( "foo", list( ",", INTEGER ), "1,    2,3, 4,   5  " );
        assertThat( setting4.apply( map( stringMap() ) ).toString(), equalTo( "[1, 2, 3, 4, 5]" ) );

        Setting<List<Integer>> setting5 = setting( "foo", list( ",", INTEGER ), "1,    2,3, 4,   " );
        assertThat( setting5.apply( map( stringMap() ) ).toString(), equalTo( "[1, 2, 3, 4]" ) );
    }

    @Test
    public void testStringList()
    {
        Setting<List<String>> setting1 = setting( "apa", STRING_LIST, "foo,bar,baz" );
        assertEquals( Arrays.asList( "foo", "bar", "baz" ), setting1.apply( map( stringMap() ) ) );

        Setting<List<String>> setting2 = setting( "apa", STRING_LIST, "foo,  bar, BAZ   " );
        assertEquals( Arrays.asList( "foo", "bar", "BAZ" ), setting2.apply( map( stringMap() ) ) );

        Setting<List<String>> setting3 = setting( "apa", STRING_LIST, "" );
        assertEquals( Collections.emptyList(), setting3.apply( map( stringMap() ) ) );
    }

    @Test
    public void testMin()
    {
        Setting<Integer> setting = setting( "foo", INTEGER, "3", min( 2 ) );

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "4" ) ) ), equalTo( 4 ) );

        // Bad
        try
        {
            setting.apply( map( stringMap( "foo", "1" ) ) );
            fail();
        }
        catch ( InvalidSettingException e )
        {
            // Ok
        }

    }

    @Test
    public void testMax()
    {
        Setting<Integer> setting = setting( "foo", INTEGER, "3", max( 5 ) );

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "4" ) ) ), equalTo( 4 ) );

        // Bad
        try
        {
            setting.apply( map( stringMap( "foo", "7" ) ) );
            fail();
        }
        catch ( InvalidSettingException e )
        {
            // Ok
        }
    }

    @Test
    public void testRange()
    {
        Setting<Integer> setting = setting( "foo", INTEGER, "3", range( 2, 5 ) );

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "4" ) ) ), equalTo( 4 ) );

        // Bad
        try
        {
            setting.apply( map( stringMap( "foo", "1" ) ) );
            fail();
        }
        catch ( InvalidSettingException e )
        {
            // Ok
        }

        try
        {
            setting.apply( map( stringMap( "foo", "6" ) ) );
            fail();
        }
        catch ( InvalidSettingException e )
        {
            // Ok
        }
    }

    @Test
    public void testMatches()
    {
        Setting<String> setting = setting( "foo", STRING, "abc", matches( "a*b*c*" ) );

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "aaabbbccc" ) ) ), equalTo( "aaabbbccc" ) );

        // Bad
        try
        {
            setting.apply( map( stringMap( "foo", "cba" ) ) );
            fail();
        }
        catch ( InvalidSettingException e )
        {
            // Ok
        }
    }

    @Test( expected = InvalidSettingException.class )
    public void testDurationWithBrokenDefault()
    {
        // Notice that the default value is less that the minimum
        Setting<Long> setting = setting( "foo.bar", DURATION, "1s", min( DURATION.apply( "3s" ) ) );
        setting.apply( map( stringMap() ) );
    }

    @Test( expected = InvalidSettingException.class )
    public void testDurationWithValueNotWithinConstraint()
    {
        Setting<Long> setting = setting( "foo.bar", DURATION, "3s", min( DURATION.apply( "3s" ) ) );
        setting.apply( map( stringMap( "foo.bar", "2s" ) ) );
    }

    @Test
    public void testDuration()
    {
        Setting<Long> setting = setting( "foo.bar", DURATION, "3s", min( DURATION.apply( "3s" ) ) );
        assertThat( setting.apply( map( stringMap( "foo.bar", "4s" ) ) ), equalTo( 4000L ) );
    }

    @Test
    public void testDefault()
    {
        Setting<Integer> setting = setting( "foo", INTEGER, "3" );

        // Ok
        assertThat( setting.apply( map( stringMap() ) ), equalTo( 3 ) );
    }

    @Test
    public void testMandatory()
    {
        Setting<Integer> setting = setting( "foo", INTEGER, MANDATORY );

        // Check that missing mandatory setting throws exception
        try
        {
            setting.apply( map( stringMap() ) );
            fail();
        }
        catch ( Exception e )
        {
            // Ok
        }
    }

    @Test
    public void testPaths()
    {
        Setting<File> home = setting( "home", PATH, "." );
        Setting<File> config = setting( "config", PATH, "config.properties", basePath( home ), isFile );
        assertThat( config.apply( map( stringMap() ) ).getAbsolutePath(),
            equalTo( new File( ".", "config.properties" ).getAbsolutePath() ) );
    }

    @Test
    public void testInheritOneLevel()
    {
        Setting<Integer> root = setting( "root", INTEGER, "4" );
        Setting<Integer> setting = setting( "foo", INTEGER, root );

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "1" ) ) ), equalTo( 1 ) );
        assertThat( setting.apply( map( stringMap() ) ), equalTo( 4 ) );
    }

    @Test
    public void testInheritHierarchy()
    {
        // Test hierarchies
        Setting<String> a = setting( "A", STRING, "A" ); // A defaults to A
        Setting<String> b = setting( "B", STRING, "B", a ); // B defaults to B unless A is defined
        Setting<String> c = setting( "C", STRING, "C", b ); // C defaults to C unless B is defined
        Setting<String> d = setting( "D", STRING, b ); // D defaults to B
        Setting<String> e = setting( "E", STRING, d ); // E defaults to D (hence B)

        assertThat( c.apply( map( stringMap( "C", "X" ) ) ), equalTo( "X" ) );
        assertThat( c.apply( map( stringMap( "B", "X" ) ) ), equalTo( "X" ) );
        assertThat( c.apply( map( stringMap( "A", "X" ) ) ), equalTo( "X" ) );
        assertThat( c.apply( map( stringMap( "A", "Y", "B", "X" ) ) ), equalTo( "X" ) );

        assertThat( d.apply( map( stringMap() ) ), equalTo( "B" ) );
        assertThat( e.apply( map( stringMap() ) ), equalTo( "B" ) );

    }

    @Test( expected = IllegalArgumentException.class )
    public void testMandatoryApplyToInherited()
    {
        // Check that mandatory settings fail even in inherited cases
        Setting<String> x = setting( "X", STRING, NO_DEFAULT );
        Setting<String> y = setting( "Y", STRING, MANDATORY, x );

        y.apply( Functions.<String, String>nullFunction() );
    }

    @Test
    public void testLogicalLogRotationThreshold() throws Exception
    {
        // WHEN
        Setting<Long> setting = GraphDatabaseSettings.logical_log_rotation_threshold;
        long defaultValue = setting.apply( map( stringMap() ) );
        long megaValue = setting.apply( map( stringMap( setting.name(), "10M" ) ) );
        long gigaValue = setting.apply( map( stringMap( setting.name(), "10g" ) ) );

        // THEN
        assertThat( defaultValue, greaterThan( 0L ) );
        assertEquals( 10 * 1024 * 1024, megaValue );
        assertEquals( 10L * 1024 * 1024 * 1024, gigaValue );
    }

    @Test
    public void testNormalizedRelativeURI() throws Exception
    {
        // Given
        Setting<URI> uri = setting( "mySetting", NORMALIZED_RELATIVE_URI, "http://localhost:7474///db///data///" );

        // When && then
        assertThat( uri.apply( Functions.<String,String>constant( null ) ).toString(), equalTo( "/db/data" ) );
    }
}
