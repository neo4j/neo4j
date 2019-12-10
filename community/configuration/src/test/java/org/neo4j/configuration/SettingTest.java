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
package org.neo4j.configuration;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.string.SecureString;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.neo4j.configuration.SettingConstraints.PORT;
import static org.neo4j.configuration.SettingConstraints.POWER_OF_2;
import static org.neo4j.configuration.SettingConstraints.any;
import static org.neo4j.configuration.SettingConstraints.except;
import static org.neo4j.configuration.SettingConstraints.is;
import static org.neo4j.configuration.SettingConstraints.matches;
import static org.neo4j.configuration.SettingConstraints.max;
import static org.neo4j.configuration.SettingConstraints.min;
import static org.neo4j.configuration.SettingConstraints.range;
import static org.neo4j.configuration.SettingValueParsers.BOOL;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.DURATION;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.HOSTNAME_PORT;
import static org.neo4j.configuration.SettingValueParsers.INT;
import static org.neo4j.configuration.SettingValueParsers.LONG;
import static org.neo4j.configuration.SettingValueParsers.NORMALIZED_RELATIVE_URI;
import static org.neo4j.configuration.SettingValueParsers.PATH;
import static org.neo4j.configuration.SettingValueParsers.SECURE_STRING;
import static org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.TIMEZONE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.configuration.SettingValueParsers.listOf;
import static org.neo4j.configuration.SettingValueParsers.ofEnum;
import static org.neo4j.configuration.SettingValueParsers.ofPartialEnum;

class SettingTest
{
    @Test
    void testSuffix()
    {
        var setting1 = (SettingImpl<Integer>) setting( "setting", INT );
        assertEquals( "setting", setting1.suffix() );
        var setting2 = (SettingImpl<Integer>) setting( "setting.suffix", INT );
        assertEquals( "suffix", setting2.suffix() );
        var setting3 = (SettingImpl<Integer>) setting( "", INT );
        assertEquals( "", setting3.suffix() );
        var setting4 = (SettingImpl<Integer>) setting( null, INT );
        assertNull( setting4.suffix() );
    }

    @Test
    void testInteger()
    {
        var setting = (SettingImpl<Integer>) setting( "setting", INT );
        assertEquals( 5, setting.parse( "5" ) );
        assertEquals( -76, setting.parse( "-76" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "foo" ) );
    }

    @Test
    void testLong()
    {
        var setting = (SettingImpl<Long>) setting( "setting", LONG );
        assertEquals( 112233445566778899L, setting.parse( "112233445566778899" ) );
        assertEquals( -112233445566778899L, setting.parse( "-112233445566778899" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "foo" ) );
    }

    @Test
    void testString()
    {
        var setting = (SettingImpl<String>) setting( "setting", STRING );
        assertEquals( "foo", setting.parse( "foo" ) );
        assertEquals( "bar", setting.parse( "  bar   " ) );
    }

    @Test
    void testSecureString()
    {
        var setting = (SettingImpl<SecureString>) setting( "setting", SECURE_STRING );
        assertEquals( "foo", setting.parse( "foo" ).getString() );
        assertNotEquals( "foo", setting.parse( "foo" ).toString() );
        assertEquals( "bar", setting.parse( "  bar   " ).getString() );
        assertNotEquals( "foo", setting.valueToString( setting.parse( "foo" ) ) );
    }

    @Test
    void testDouble()
    {
        BiFunction<Double,Double,Boolean> compareDoubles = ( Double d1, Double d2 ) -> Math.abs( d1 - d2 ) < 0.000001;

        var setting = (SettingImpl<Double>) setting( "setting", DOUBLE );
        assertEquals( 5.0, setting.parse( "5" ) );
        assertTrue( compareDoubles.apply( -.123, setting.parse( "-0.123" ) ) );
        assertTrue( compareDoubles.apply( 5.0, setting.parse( "5" ) ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "foo" ) );
    }

    @Test
    void testList()
    {
        var setting = (SettingImpl<List<Integer>>) setting( "setting", listOf( INT ) );
        assertEquals( 5, setting.parse( "5" ).get( 0 ) );
        assertEquals( 0, setting.parse( "" ).size() );
        assertEquals( 4, setting.parse( "5, 31, -4  ,2" ).size() );
        assertEquals( Arrays.asList( 4, 2, 3, 1 ), setting.parse( "4,2,3,1" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "2,3,foo,7" ) );

        assertFalse( setting.valueToString( setting.parse( "4,2,3,1" ) ).startsWith( "[" ) );
        assertFalse( setting.valueToString( setting.parse( "4,2,3,1" ) ).endsWith( "]" ) );
    }

    @Test
    void testEnum()
    {
        var setting = (SettingImpl<Colors>) setting( "setting", ofEnum( Colors.class ) );
        assertEquals( Colors.BLUE, setting.parse( "BLUE" ) );
        assertEquals( Colors.GREEN, setting.parse( "gReEn" ) );
        assertEquals( Colors.RED, setting.parse( "red" ));
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "orange" ) );
    }

    @Test
    void testPartialEnum()
    {
        var setting = (SettingImpl<Colors>) setting( "setting", ofPartialEnum( Colors.GREEN, Colors.BLUE ) );
        assertEquals( Colors.BLUE, setting.parse( "BLUE" ) );
        assertEquals( Colors.GREEN, setting.parse( "gReEn" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "red" ) );
    }

    @Test
    void testStringEnum()
    {
        var setting = (SettingImpl<StringEnum>) setting( "setting", ofEnum( StringEnum.class ) );
        assertEquals( StringEnum.DEFAULT, setting.parse( "default" ) );
        assertEquals( StringEnum.V_1, setting.parse( "1.0" ) );
        assertEquals( StringEnum.V_1_1, setting.parse( "1.1" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "orange" ) );

    }

    @Test
    void testBool()
    {
        var setting = (SettingImpl<Boolean>) setting( "setting", BOOL );
        assertTrue( setting.parse( "True" ) );
        assertFalse( setting.parse( "false" ) );
        assertFalse( setting.parse( "false" ) );
        assertFalse( setting.parse( FALSE ) );
        assertTrue( setting.parse( TRUE ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "foo" ) );
    }

    @Test
    void testDuration()
    {
        var setting = (SettingImpl<Duration>) setting( "setting", DURATION );
        assertEquals( 60, setting.parse( "1m" ).toSeconds() );
        assertEquals( 1000, setting.parse( "1s" ).toMillis() );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "foo" ) );

        assertEquals( "1s", setting.valueToString( setting.parse( "1s" ) ) );
        assertEquals( "3m", setting.valueToString( setting.parse( "3m" ) ) );
        assertEquals( "0ns", setting.valueToString( setting.parse( "0s" ) ) );
    }

    @Test
    void testHostnamePort()
    {
        var setting = (SettingImpl<HostnamePort>) setting( "setting", HOSTNAME_PORT );
        assertEquals( new HostnamePort( "localhost", 7474 ), setting.parse( "localhost:7474" ) );
        assertEquals( new HostnamePort( "localhost", 1000, 2000 ), setting.parse( "localhost:1000-2000" ) );
        assertEquals( new HostnamePort( "localhost" ), setting.parse( "localhost" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "localhost:5641:7474" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "localhost:foo" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "7474:localhost" ) );
    }

    @Test
    void testTimeZone()
    {
        var setting = (SettingImpl<ZoneId>) setting( "setting", TIMEZONE );
        assertEquals( ZoneId.from( ZoneOffset.UTC ), setting.parse( "+00:00" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "foo" ) );
    }

    @Test
    void testSocket()
    {
        var setting = (SettingImpl<SocketAddress>) setting( "setting", SOCKET_ADDRESS );
        assertEquals( new SocketAddress( "127.0.0.1", 7474 ), setting.parse( "127.0.0.1:7474" ) );
        assertEquals( new SocketAddress( "127.0.0.1", -1 ), setting.parse( "127.0.0.1" ) );
        assertEquals( new SocketAddress( null, 7474 ), setting.parse( ":7474" ) );
    }

    @Test
    void testSocketSolve()
    {
        var setting = (SettingImpl<SocketAddress>) setting( "setting", SOCKET_ADDRESS );
        assertEquals( new SocketAddress( "localhost", 7473 ),setting.solveDependency( setting.parse( "localhost:7473" ),  setting.parse( "127.0.0.1:7474" ) ) );
        assertEquals( new SocketAddress( "127.0.0.1", 7473 ),setting.solveDependency( setting.parse( ":7473" ),  setting.parse( "127.0.0.1:7474" ) ) );
        assertEquals( new SocketAddress( "127.0.0.1", 7473 ),setting.solveDependency( setting.parse( ":7473" ),  setting.parse( "127.0.0.1" ) ) );
        assertEquals( new SocketAddress( "localhost", 7474 ),setting.solveDependency( setting.parse( "localhost" ),  setting.parse( ":7474" ) ) );
        assertEquals( new SocketAddress( "localhost", 7474 ),setting.solveDependency( setting.parse( "localhost" ),  setting.parse( "127.0.0.1:7474" ) ) );
        assertEquals( new SocketAddress( "localhost", 7474 ),setting.solveDependency( null,  setting.parse( "localhost:7474" ) ) );
    }

    @Test
    void testBytes()
    {
        var setting = (SettingImpl<Long>) setting( "setting", BYTES );
        assertEquals( 2048, setting.parse( "2k" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "1gig" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.parse( "-1M" ) );
    }

    @Test
    void testURI()
    {
        var setting = (SettingImpl<URI>) setting( "setting", SettingValueParsers.URI );
        assertEquals( URI.create( "/path/to/../something/" ), setting.parse( "/path/to/../something/" ) );
    }

    @Test
    void testNormalizedRelativeURI()
    {
        var setting = (SettingImpl<URI>) setting( "setting", NORMALIZED_RELATIVE_URI );
        assertEquals( URI.create( "/path/to/something" ), setting.parse( "/path/away/from/../../to/something/" ) );
    }

    @Test
    void testPath()
    {
        var setting = (SettingImpl<Path>) setting( "setting", PATH );
        assertEquals( Path.of( "/absolute/path" ), setting.parse( "/absolute/path" ) );
        assertEquals( Path.of( "/absolute/path" ), setting.parse( "/absolute/wrong/../path" ) );
    }

    @Test
    void testSolvePath()
    {
        var setting = (SettingImpl<Path>) setting( "setting", PATH );
        assertEquals( Path.of( "/base/path/to/file" ).toAbsolutePath(),
                setting.solveDependency( setting.parse( "to/file" ), setting.parse( "/base/path" ).toAbsolutePath() ) );
        assertEquals( Path.of( "/to/file" ).toAbsolutePath(),
                setting.solveDependency( setting.parse( "/to/file" ), setting.parse( "/base/path" ).toAbsolutePath() ) );
        assertEquals( Path.of( "/base/path/" ).toAbsolutePath(),
                setting.solveDependency( setting.parse( "" ), setting.parse( "/base/path/" ).toAbsolutePath() ) );
        assertEquals( Path.of( "/base/path" ).toAbsolutePath(), setting.solveDependency( setting.parse( "path" ), setting.parse( "/base" ).toAbsolutePath() ) );
        assertEquals( Path.of( "/base" ).toAbsolutePath(), setting.solveDependency( null, setting.parse( "/base" ).toAbsolutePath() ) );
        assertThrows( IllegalArgumentException.class, () -> setting.solveDependency( setting.parse( "path" ), setting.parse( "base" ) ) );
    }

    @Test
    void testDefaultSolve()
    {
        var defaultSolver = new SettingValueParser<String>()
        {
            @Override
            public String parse( String value )
            {
                return value;
            }

            @Override
            public String getDescription()
            {
                return "default solver";
            }

            @Override
            public Class<String> getType()
            {
                return String.class;
            }
        };

        var setting = (SettingImpl<String>) setting( "setting", defaultSolver );
        assertEquals( "foo", setting.solveDependency( "foo", "bar" ) );
        assertEquals( "bar", setting.solveDependency( null, "bar" ) );
        assertEquals( "foo", setting.solveDependency( "foo", null ) );
        assertNull( setting.solveDependency( null, null ) );
    }

    @Test
    void testMinConstraint()
    {
        var setting = (SettingImpl<Integer>) settingBuilder( "setting", INT ).addConstraint( min( 10 ) ).build();
        assertDoesNotThrow( () -> setting.validate( 100 ) );
        assertDoesNotThrow( () -> setting.validate( 10 ) );
        assertThrows( IllegalArgumentException.class, () -> setting.validate( 9 ) );
    }

    @Test
    void testMaxConstraint()
    {
        var setting = (SettingImpl<Integer>) settingBuilder( "setting", INT ).addConstraint( max( 10 ) ).build();
        assertDoesNotThrow( () -> setting.validate( -100 ) );
        assertDoesNotThrow( () -> setting.validate( 10 ) );
        assertThrows( IllegalArgumentException.class, () -> setting.validate( 11 ) );
    }

    @Test
    void testRangeConstraint()
    {
        var setting = (SettingImpl<Double>) settingBuilder( "setting", DOUBLE ).addConstraint( range( 10.0, 20.0 ) ).build();

        assertThrows( IllegalArgumentException.class, () -> setting.validate( 9.9 ) );
        assertThrows( IllegalArgumentException.class, () -> setting.validate( 20.01 ) );
        assertDoesNotThrow( () -> setting.validate( 10.1 ) );
        assertDoesNotThrow( () -> setting.validate( 19.9999 ) );
    }

    @Test
    void testExceptConstraint()
    {
        var setting = (SettingImpl<String>) settingBuilder( "setting", STRING ).addConstraint( except( "foo" ) ).build();
        assertThrows( IllegalArgumentException.class, () -> setting.validate( "foo" ) );
        assertDoesNotThrow( () -> setting.validate( "bar" ) );
    }

    @Test
    void testMatchesConstraint()
    {
        var setting = (SettingImpl<String>) settingBuilder( "setting", STRING ).addConstraint( matches( "^[^.]+\\.[^.]+$" ) ).build();
        assertDoesNotThrow( () -> setting.validate( "foo.bar" ) );
        assertThrows( IllegalArgumentException.class, () -> setting.validate( "foo" ) );
    }

    @Test
    void testPowerOf2Constraint()
    {
        var setting = (SettingImpl<Long>) settingBuilder( "setting", LONG ).addConstraint( POWER_OF_2 ).build();
        assertDoesNotThrow( () -> setting.validate( 8L ) );
        assertDoesNotThrow( () -> setting.validate( 4294967296L ) );
        assertThrows( IllegalArgumentException.class, () -> setting.validate( 1023L ) );
    }

    @Test
    void testPortConstraint()
    {
        var setting = (SettingImpl<Integer>) settingBuilder( "setting", INT ).addConstraint( PORT ).build();
        assertDoesNotThrow( () -> setting.validate( 7474 ) );
        assertThrows( IllegalArgumentException.class, () -> setting.validate( 200000 ) );
        assertThrows( IllegalArgumentException.class, () -> setting.validate( -1 ) );
    }

    @Test
    void testIsConstraint()
    {
        var setting = (SettingImpl<Integer>) settingBuilder( "setting", INT ).addConstraint( is( 10 ) ).build();
        assertDoesNotThrow( () -> setting.validate( 10 ) );
        assertThrows( IllegalArgumentException.class, () -> setting.validate( 9 ) );
    }

    @Test
    void testAnyConstraint()
    {
        var intSetting = (SettingImpl<Integer>) settingBuilder( "setting", INT )
                .addConstraint( any( min( 30 ), is( 0 ), is( -10 ) )  ).build();
        assertDoesNotThrow( () -> intSetting.validate( 30 ) );
        assertDoesNotThrow( () -> intSetting.validate( 100 ) );
        assertDoesNotThrow( () -> intSetting.validate( 0 ) );
        assertDoesNotThrow( () -> intSetting.validate( -10 ) );
        assertThrows( IllegalArgumentException.class, () -> intSetting.validate( 29 ) );
        assertThrows( IllegalArgumentException.class, () -> intSetting.validate( 1 ) );
        assertThrows( IllegalArgumentException.class, () -> intSetting.validate( -9 ) );

        var durationSetting = (SettingImpl<Duration>) settingBuilder( "setting", DURATION )
                .addConstraint( any( min( Duration.ofMinutes( 30 ) ), is( Duration.ZERO ) )  ).build();
        assertDoesNotThrow( () -> durationSetting.validate( Duration.ofMinutes( 30 ) ) );
        assertDoesNotThrow( () -> durationSetting.validate( Duration.ofHours( 1 ) ) );
        assertDoesNotThrow( () -> durationSetting.validate( Duration.ZERO ) );
        assertThrows( IllegalArgumentException.class, () -> durationSetting.validate( Duration.ofMinutes( 29 ) ) );
        assertThrows( IllegalArgumentException.class, () -> durationSetting.validate( Duration.ofMillis( 1 ) ) );

    }

    @Test
    void testDescriptionWithConstraints()
    {
        var oneConstraintSetting = (SettingImpl<Long>) settingBuilder( "setting.name", LONG )
                .addConstraint( POWER_OF_2 )
                .build();

        var twoConstraintSetting = (SettingImpl<Integer>) settingBuilder( "setting.name", INT )
                .addConstraint( min( 2 ) )
                .addConstraint( max( 10 ) )
                .build();

        assertEquals( "setting.name, a long which is power of 2", oneConstraintSetting.description() );
        assertEquals( "setting.name, an integer which is minimum `2` and is maximum `10`", twoConstraintSetting.description() );
    }

    @TestFactory
    Collection<DynamicTest> testDescriptionDependency()
    {
        Collection<DynamicTest> tests = new ArrayList<>();
        tests.add( dynamicTest( "Test int dependency description",
                () -> testDescDependency( INT, "setting.child, an integer. If unset the value is inherited from setting.parent" ) ) );
        tests.add( dynamicTest( "Test socket dependency description", () -> testDescDependency( SOCKET_ADDRESS,
                "setting.child, a socket address. If missing port or hostname it is acquired from setting.parent" ) ) );
        tests.add( dynamicTest( "Test path dependency description",
                () -> testDescDependency( PATH, "setting.child, a path. If relative it is resolved from setting.parent" ) ) );
        return tests;
    }

    private static <T> void testDescDependency( SettingValueParser<T> parser, String expectedDescription )
    {
        var parent = settingBuilder( "setting.parent", parser ).immutable().build();
        var child = settingBuilder( "setting.child", parser ).setDependency( parent ).build();

        assertEquals( expectedDescription, child.description() );
    }

    private static <T> SettingImpl.Builder<T> settingBuilder( String name, SettingValueParser<T> parser )
    {
        return SettingImpl.newBuilder( name, parser, null );
    }

    private static <T> SettingImpl<T> setting( String name, SettingValueParser<T> parser )
    {
        return (SettingImpl<T>) SettingImpl.newBuilder( name, parser, null ).build();
    }

    private enum Colors
    {
        BLUE, GREEN, RED;
    }
    private enum StringEnum
    {
        DEFAULT( "default" ), V_1( "1.0" ), V_1_1( "1.1" );

        private final String name;
        StringEnum( String name )
        {
            this.name = name;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }
}
