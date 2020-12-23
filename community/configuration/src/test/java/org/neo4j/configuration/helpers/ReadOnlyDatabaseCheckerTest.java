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
package org.neo4j.configuration.helpers;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Setting;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;

class ReadOnlyDatabaseCheckerTest
{
    @Test
    void globalReadOnlyConfigHasHigherPriorityThanReadOnlyDatabaseList()
    {
        // given
        var configValues = Map.of( read_only_database_default, true,
                                   read_only_databases, Set.of() );
        var config = Config.defaults( configValues );
        var checker = new ReadOnlyDatabaseChecker.Default( config );

        //when/then
        assertTrue( checker.test( "foo1234" ) );
    }

    @Test
    void readOnlyDatabaseShouldBeTakenIntoAccountWhenGlobalReadOnlyConfigIsOff()
    {
        // given
        final String fooDb = "foo";
        var configValues = Map.of( read_only_database_default, false,
                                   read_only_databases, Set.of( fooDb ) );
        var config = Config.defaults( configValues );
        var checker = new ReadOnlyDatabaseChecker.Default( config );

        //when/then
        assertTrue( checker.test( fooDb ) );
        assertFalse( checker.test( "test12356" ) );
    }

    @Test
    void shouldBeNotPossibleToMakeSystemDbReadOnly()
    {
        // given
        var configValues = Map.of( read_only_database_default, false,
                                   read_only_databases, Set.of( SYSTEM_DATABASE_NAME ) );

        // when/then
        assertThrows( IllegalArgumentException.class, () -> Config.defaults( configValues ) );
    }

    @Test
    void systemDatabaseCantBeSetReadOnlyDynamically()
    {
        // given
        var configValues = Map.of( read_only_database_default, false,
                                   read_only_databases, Set.of( DEFAULT_DATABASE_NAME ) );

        var config = Config.defaults( configValues );
        var checker = new ReadOnlyDatabaseChecker.Default( config );

        // when/then
        assertFalse( checker.test( SYSTEM_DATABASE_NAME ) );

        // when configs are changed
        assertThrows( IllegalArgumentException.class,
                      () -> config.setDynamic( read_only_databases, Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME ), getClass().getSimpleName() ) );

        // then
        assertFalse( checker.test( SYSTEM_DATABASE_NAME ) );
    }

    @Test
    void distinguishesBetweenFixedAndDynamicReadOnly()
    {
        // given
        var fixedGlobalReadOnly = Map.<Setting<?>, Object>of( read_only, true );
        var dynamicGlobalReadOnly = Map.<Setting<?>, Object>of( read_only_database_default, true );

        var fixedChecker = new ReadOnlyDatabaseChecker.Default( Config.defaults( fixedGlobalReadOnly ) );
        var dynamicChecker = new ReadOnlyDatabaseChecker.Default( Config.defaults( dynamicGlobalReadOnly ) );

        // when/then
        assertTrue( fixedChecker.test( "foo" ) );
        assertTrue( dynamicChecker.test( "foo" ) );

        assertTrue( fixedChecker.readOnlyFixed() );
        assertFalse( dynamicChecker.readOnlyFixed() );
    }

    @Test
    void readOnlyConfigsShouldBeDynamic()
    {
        // given
        var configValues = Map.of( read_only_database_default, false,
                                   read_only_databases, Set.of( DEFAULT_DATABASE_NAME ) );

        var config = Config.defaults( configValues );
        var roDatabaseList = Set.of( DEFAULT_DATABASE_NAME, "foo" );
        var writableDatabaseList = Set.of( "bar", "baz" );

        // when configs are changed
        config.setDynamic( read_only_database_default, true, getClass().getSimpleName() );
        config.setDynamic( read_only_databases, roDatabaseList, getClass().getSimpleName() );
        config.setDynamic( writable_databases, writableDatabaseList, getClass().getSimpleName() );

        // then
        assertEquals( roDatabaseList, config.get( read_only_databases ) );
        assertTrue( config.get( read_only_database_default ) );
        assertEquals( writableDatabaseList, config.get( writable_databases ) );
    }

    @Test
    void writableShouldOverrideReadOnlyDefault()
    {
        // given
        var configValues = Map.of(
                read_only_database_default, true,
                writable_databases, Set.of( "foo"  ) );

        var config = Config.defaults( configValues );
        var checker = new ReadOnlyDatabaseChecker.Default( config );

        // when/then
        assertTrue( checker.test( "bar" ) );
        assertFalse( checker.test( "foo" ) );
    }

    @Test
    void explicitReadOnlyShouldOverrideExplicitWritable()
    {
        // given
        Map<Setting<?>, Object> configValues = Map.of(
                read_only_databases, Set.of( "foo", "bar" ),
                writable_databases, Set.of( "bar", "baz" ) );

        var config = Config.defaults( configValues );
        var checker = new ReadOnlyDatabaseChecker.Default( config );

        // when/then
        assertTrue( checker.test( "bar" ) );
        assertFalse( checker.test( "baz" ) );
    }
}
