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
package org.neo4j.kernel.configuration;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.configuration.Settings.DURATION;
import static org.neo4j.kernel.configuration.Settings.INTEGER;
import static org.neo4j.kernel.configuration.Settings.NORMALIZED_RELATIVE_URI;
import static org.neo4j.kernel.configuration.Settings.NO_DEFAULT;
import static org.neo4j.kernel.configuration.Settings.PATH;
import static org.neo4j.kernel.configuration.Settings.STRING;
import static org.neo4j.kernel.configuration.Settings.STRING_LIST;
import static org.neo4j.kernel.configuration.Settings.buildSetting;
import static org.neo4j.kernel.configuration.Settings.except;
import static org.neo4j.kernel.configuration.Settings.list;
import static org.neo4j.kernel.configuration.Settings.matches;
import static org.neo4j.kernel.configuration.Settings.max;
import static org.neo4j.kernel.configuration.Settings.min;
import static org.neo4j.kernel.configuration.Settings.pathSetting;
import static org.neo4j.kernel.configuration.Settings.range;
import static org.neo4j.kernel.configuration.Settings.setting;

class SettingsTest
{
    @Test
    void parsesAbsolutePaths()
    {
        File absolutePath = new File( "some/path" ).getAbsoluteFile();
        File thePath = Settings.PATH.apply( absolutePath.toString() );

        assertEquals( absolutePath, thePath );
    }

    @Test
    void doesntAllowRelativePaths()
    {
        File relativePath = new File( "some/path" );

        assertThrows( IllegalArgumentException.class, () -> Settings.PATH.apply( relativePath.toString() ) );
    }

    @Test
    void pathSettingsProvideDefaultValues()
    {
        File theDefault = new File( "/some/path" ).getAbsoluteFile();
        Setting<File> setting = pathSetting( "some.setting", theDefault.getAbsolutePath() );
        assertThat( Config.defaults().get( setting ), is( theDefault ) );
    }

    @Test
    void pathSettingsAreNullIfThereIsNoValueAndNoDefault()
    {
        Setting<File> setting = pathSetting( "some.setting", NO_DEFAULT );
        assertThat( Config.defaults().get( setting ), is( nullValue() ) );
    }

    @Test
    void shouldHaveAUsefulToStringWhichIsUsedAsTheValidValuesInDocumentation()
    {
        assertThat( pathSetting( EMPTY, NO_DEFAULT ).toString(), containsString( "A filesystem path" ) );
    }

    @Test
    void testInteger()
    {
        Setting<Integer> setting = setting( "foo", INTEGER, "3" );

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "4" ) ) ), equalTo( 4 ) );

        // Bad
        assertThrows( InvalidSettingException.class, () -> setting.apply( map( stringMap( "foo", "bar" ) ) ) );
    }

    @Test
    void testList()
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
    void testStringList()
    {
        Setting<List<String>> setting1 = setting( "apa", STRING_LIST, "foo,bar,baz" );
        assertEquals( Arrays.asList( "foo", "bar", "baz" ), setting1.apply( map( stringMap() ) ) );

        Setting<List<String>> setting2 = setting( "apa", STRING_LIST, "foo,  bar, BAZ   " );
        assertEquals( Arrays.asList( "foo", "bar", "BAZ" ), setting2.apply( map( stringMap() ) ) );

        Setting<List<String>> setting3 = setting( "apa", STRING_LIST, "" );
        assertEquals( Collections.emptyList(), setting3.apply( map( stringMap() ) ) );
    }

    @Test
    void testMin()
    {
        Setting<Integer> setting = buildSetting( "foo", INTEGER, "3" ).constraint( min( 2 ) ).build();

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "4" ) ) ), equalTo( 4 ) );

        // Bad
        assertThrows( InvalidSettingException.class, () -> setting.apply( map( stringMap( "foo", "1" ) ) ) );
    }

    @Test
    void exceptDoesNotAllowForbiddenValues()
    {
        Setting<String> restrictedSetting = buildSetting( "foo", STRING, "test" ).constraint( except( "a", "b", "c" ) ).build();
        assertEquals( "test", restrictedSetting.apply( map( stringMap() ) ) );
        assertEquals( "d", restrictedSetting.apply( map( stringMap( "foo", "d" ) ) ) );
        assertThrows( InvalidSettingException.class, () -> restrictedSetting.apply( map( stringMap( "foo", "a" ) ) ) );
        assertThrows( InvalidSettingException.class, () -> restrictedSetting.apply( map( stringMap( "foo", "b" ) ) ) );
        InvalidSettingException exception =
                assertThrows( InvalidSettingException.class, () -> restrictedSetting.apply( map( stringMap( "foo", "c" ) ) ) );
        assertThat( exception.getMessage(), containsString( "not allowed value is: c" ) );
    }

    @Test
    void testMax()
    {
        Setting<Integer> setting = buildSetting( "foo", INTEGER, "3" ).constraint( max( 5 ) ).build();

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "4" ) ) ), equalTo( 4 ) );

        // Bad
        assertThrows( InvalidSettingException.class, () -> setting.apply( map( stringMap( "foo", "7" ) ) ) );
    }

    @Test
    void testRange()
    {
        Setting<Integer> setting = buildSetting( "foo", INTEGER, "3" ).constraint( range( 2, 5 ) ).build();

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "4" ) ) ), equalTo( 4 ) );

        // Bad
        assertThrows( InvalidSettingException.class, () -> setting.apply( map( stringMap( "foo", "1" ) ) ) );
        assertThrows( InvalidSettingException.class, () -> setting.apply( map( stringMap( "foo", "6" ) ) ) );
    }

    @Test
    void testMatches()
    {
        Setting<String> setting = buildSetting( "foo", STRING, "abc" ).constraint(  matches( "a*b*c*" ) ).build();

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "aaabbbccc" ) ) ), equalTo( "aaabbbccc" ) );

        // Bad
        assertThrows( InvalidSettingException.class, () -> setting.apply( map( stringMap( "foo", "cba" ) ) ) );
    }

    @Test
    void testDurationWithBrokenDefault()
    {
        // Notice that the default value is less that the minimum
        Setting<Duration> setting = buildSetting( "foo.bar", DURATION, "1s" ).constraint( min( DURATION.apply( "3s" ) ) ).build();
        assertThrows( InvalidSettingException.class, () -> setting.apply( map( stringMap() ) ) );
    }

    @Test
    void testDurationWithValueNotWithinConstraint()
    {
        Setting<Duration> setting = buildSetting( "foo.bar", DURATION, "3s" ).constraint( min( DURATION.apply( "3s" ) ) ).build();
        assertThrows( InvalidSettingException.class, () -> setting.apply( map( stringMap( "foo.bar", "2s" ) ) ) );
    }

    @Test
    void testDuration()
    {
        Setting<Duration> setting = buildSetting( "foo.bar", DURATION, "3s").constraint( min( DURATION.apply( "3s" ) ) ).build();
        assertThat( setting.apply( map( stringMap( "foo.bar", "4s" ) ) ), equalTo( Duration.ofSeconds( 4 ) ) );
    }

    @Test
    void badDurationMissingNumber()
    {
        Setting<Duration> setting = buildSetting( "foo.bar", DURATION ).build();
        InvalidSettingException exception = assertThrows( InvalidSettingException.class, () -> setting.apply( map( stringMap( "foo.bar", "ms" ) ) ) );
        assertThat( exception.getMessage(), containsString( "Missing numeric value" ) );
    }

    @Test
    void badDurationInvalidUnit()
    {
        Setting<Duration> setting = buildSetting( "foo.bar", DURATION ).build();
        InvalidSettingException exception =
                assertThrows( InvalidSettingException.class, () -> setting.apply( map( stringMap( "foo.bar", "2gigaseconds" ) ) ) );
        assertThat( exception.getMessage(), containsString( "Unrecognized unit 'gigaseconds'" ) );
    }

    @Test
    void testDefault()
    {
        Setting<Integer> setting = setting( "foo", INTEGER, "3" );

        // Ok
        assertThat( setting.apply( map( stringMap() ) ), equalTo( 3 ) );
    }

    @Test
    void testPaths()
    {
        File directory = new File( "myDirectory" );
        Setting<File> config = buildSetting( "config", PATH, new File( directory, "config.properties" ).getAbsolutePath() ).constraint(
                isFile ).build();
        assertThat( config.apply( map( stringMap() ) ).getAbsolutePath(),
                equalTo( new File( directory, "config.properties" ).getAbsolutePath() ) );
    }

    @Test
    void testInheritOneLevel()
    {
        Setting<Integer> root = setting( "root", INTEGER, "4" );
        Setting<Integer> setting = buildSetting( "foo", INTEGER ).inherits( root ).build();

        // Ok
        assertThat( setting.apply( map( stringMap( "foo", "1" ) ) ), equalTo( 1 ) );
        assertThat( setting.apply( map( stringMap() ) ), equalTo( 4 ) );
    }

    @Test
    void testInheritHierarchy()
    {
        // Test hierarchies
        Setting<String> a = setting( "A", STRING, "A" ); // A defaults to A
        Setting<String> b = buildSetting( "B", STRING, "B" ).inherits( a ).build(); // B defaults to B unless A is defined
        Setting<String> c = buildSetting( "C", STRING, "C" ).inherits( b ).build(); // C defaults to C unless B is defined
        Setting<String> d = buildSetting( "D", STRING ).inherits( b ).build(); // D defaults to B
        Setting<String> e = buildSetting( "E", STRING ).inherits( d ).build(); // E defaults to D (hence B)

        assertThat( c.apply( map( stringMap( "C", "X" ) ) ), equalTo( "X" ) );
        assertThat( c.apply( map( stringMap( "B", "X" ) ) ), equalTo( "X" ) );
        assertThat( c.apply( map( stringMap( "A", "X" ) ) ), equalTo( "X" ) );
        assertThat( c.apply( map( stringMap( "A", "Y", "B", "X" ) ) ), equalTo( "X" ) );

        assertThat( d.apply( map( stringMap() ) ), equalTo( "B" ) );
        assertThat( e.apply( map( stringMap() ) ), equalTo( "B" ) );
    }

    @Test
    void testLogicalLogRotationThreshold()
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
    void testNormalizedRelativeURI()
    {
        // Given
        Setting<URI> uri = setting( "mySetting", NORMALIZED_RELATIVE_URI, "http://localhost:7474///db///data///" );

        // When && then
        assertThat( uri.apply( always -> null ).toString(), equalTo( "/db/data" ) );
    }

    @Test
    void onlySingleInheritanceShouldBeAllowed()
    {
        Setting<String> a = setting( "A", STRING, "A" );
        Setting<String> b = setting( "B", STRING, "B" );
        assertThrows( AssertionError.class, () -> buildSetting( "C", STRING, "C" ).inherits( a ).inherits( b ).build() );
    }

    private static <From, To> Function<From,To> map( final Map<From,To> map )
    {
        return map::get;
    }

    private static BiFunction<File,Function<String,String>,File> isFile = ( path, settings ) ->
    {
        if ( path.exists() && !path.isFile() )
        {
            throw new IllegalArgumentException(
                    String.format( "%s must point to a file, not a directory", path.toString() ) );
        }

        return path;
    };
}
