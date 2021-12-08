/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.dbms.database.readonly;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory;
import org.neo4j.configuration.database.readonly.ConfigReadOnlyDatabaseListener;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_databases;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;

@ExtendWith( LifeExtension.class )
class ReadOnlyDatabasesTest
{

    @Inject
    private LifeSupport life;

    @Test
    void globalReadOnlyConfigHasHigherPriorityThanReadOnlyDatabaseList()
    {
        // given
        var fooDb = DatabaseIdFactory.from( "foo1234", UUID.randomUUID() );
        var configValues = Map.of( read_only_database_default, true,
                                   read_only_databases, Set.of() );
        var config = Config.defaults( configValues );
        DatabaseIdRepository databaseIdRepository = mock( DatabaseIdRepository.class );
        Mockito.when( databaseIdRepository.getByName( "foo12345" ) ).thenReturn( Optional.of(fooDb) );
        var readOnlyLookup =  new ConfigBasedLookupFactory( config, databaseIdRepository );
        var checker = new ReadOnlyDatabases( readOnlyLookup );
        var listener = new ConfigReadOnlyDatabaseListener( checker, config );
        life.add( listener );

        //when/then
        assertTrue( checker.isReadOnly( fooDb ) );
    }

    @Test
    void readOnlyDatabaseShouldBeTakenIntoAccountWhenGlobalReadOnlyConfigIsOff()
    {
        // given
        var fooDb = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var configValues = Map.of( read_only_database_default, false,
                                   read_only_databases, Set.of( fooDb.name() ) );
        var config = Config.defaults( configValues );

        DatabaseIdRepository databaseIdRepository = mock( DatabaseIdRepository.class );
        Mockito.when( databaseIdRepository.getByName( "foo" ) ).thenReturn( Optional.of(fooDb) );
        var readOnlyLookup = new ConfigBasedLookupFactory( config, databaseIdRepository );
        var checker = new ReadOnlyDatabases( readOnlyLookup );
        var listener = new ConfigReadOnlyDatabaseListener( checker, config );
        life.add( listener );

        //when/then
        assertTrue( checker.isReadOnly( fooDb ) );
        assertFalse( checker.isReadOnly( DatabaseIdFactory.from( "test12356", UUID.randomUUID() ) ) );
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
        var defaultDatabase = DatabaseIdFactory.from( DEFAULT_DATABASE_NAME, UUID.randomUUID() );
        var configValues = Map.of( read_only_database_default, false,
                                   read_only_databases, Set.of( DEFAULT_DATABASE_NAME ) );

        var config = Config.defaults( configValues );
        DatabaseIdRepository databaseIdRepository = mock( DatabaseIdRepository.class );
        Mockito.when( databaseIdRepository.getByName( DEFAULT_DATABASE_NAME ) ).thenReturn( Optional.of( defaultDatabase ) );
        Mockito.when( databaseIdRepository.getByName( SYSTEM_DATABASE_NAME ) ).thenReturn( Optional.of( NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID ) );
        var readOnlyLookup = new ConfigBasedLookupFactory( config, databaseIdRepository );
        var checker = new ReadOnlyDatabases( readOnlyLookup );

        // when/then
        assertFalse( checker.isReadOnly( NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID ) );

        // when configs are changed
        assertThrows( IllegalArgumentException.class,
                      () -> config.setDynamic( read_only_databases, Set.of( DEFAULT_DATABASE_NAME, SYSTEM_DATABASE_NAME ), getClass().getSimpleName() ) );

        // then
        assertFalse( checker.isReadOnly( NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID ) );
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
        var foo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var bar = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
        var databases = Set.of( foo, bar );
        var configValues = Map.of(
                read_only_database_default, true,
                writable_databases, Set.of( foo.name() ) );
        var config = Config.defaults( configValues );
        DatabaseIdRepository databaseIdRepository = mock( DatabaseIdRepository.class );
        Mockito.when( databaseIdRepository.getByName( "foo" ) ).thenReturn( Optional.of( foo ) );
        Mockito.when( databaseIdRepository.getByName( "bar" ) ).thenReturn( Optional.of( bar ) );
        var readOnlyLookup = new ConfigBasedLookupFactory( config, databaseIdRepository );
        var checker = new ReadOnlyDatabases( readOnlyLookup );
        var listener = new ConfigReadOnlyDatabaseListener( checker, config );
        life.add( listener );

        // when/then
        assertTrue( checker.isReadOnly( bar ) );
        assertFalse( checker.isReadOnly( foo ) );
    }

    @Test
    void explicitReadOnlyShouldOverrideExplicitWritable()
    {
        // given
        var foo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var bar = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
        var baz = DatabaseIdFactory.from( "baz", UUID.randomUUID() );
        var databases = Set.of( foo, bar, baz );
        Map<Setting<?>, Object> configValues = Map.of(
                read_only_databases, Set.of( foo.name(), bar.name() ),
                writable_databases, Set.of( bar.name(), baz.name() ) );

        var config = Config.defaults( configValues );
        DatabaseIdRepository databaseIdRepository = mock( DatabaseIdRepository.class );
        Mockito.when( databaseIdRepository.getByName( "foo" ) ).thenReturn( Optional.of( foo ) );
        Mockito.when( databaseIdRepository.getByName( "bar" ) ).thenReturn( Optional.of( bar ) );
        Mockito.when( databaseIdRepository.getByName( "baz" ) ).thenReturn( Optional.of( baz ) );
        var readOnlyLookup = new ConfigBasedLookupFactory( config, databaseIdRepository );
        var checker = new ReadOnlyDatabases( readOnlyLookup );
        var listener = new ConfigReadOnlyDatabaseListener( checker, config );
        life.add( listener );

        // when/then
        assertTrue( checker.isReadOnly( bar ) );
        assertFalse( checker.isReadOnly( baz ) );
    }

    @Test
    void refreshShouldReloadReadOnlyFromLookups()
    {
        // given
        var foo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var bar = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
        var readOnlyDatabaseNames = new HashSet<NamedDatabaseId>();
        readOnlyDatabaseNames.add( foo );
        var readOnly = new ReadOnlyDatabases( () -> {
            var snapshot = Set.copyOf( readOnlyDatabaseNames );
            return snapshot::contains;
        } );

        // when
        readOnly.refresh();
        assertTrue( readOnly.isReadOnly( foo ) );
        assertFalse( readOnly.isReadOnly( bar ) );

        // given
        readOnlyDatabaseNames.add( bar );
        assertFalse( readOnly.isReadOnly( bar ) );

        // when
        readOnly.refresh();

        // then
        assertTrue( readOnly.isReadOnly( bar ) );
    }

    @Test
    void refreshShouldIncrementUpdateId()
    {
        // given
        var readOnlyDatabases = new ReadOnlyDatabases();
        var originalUpdateId = readOnlyDatabases.updateId();

        // when
        readOnlyDatabases.refresh();

        // then
        assertThat( readOnlyDatabases.updateId() ).isEqualTo( originalUpdateId + 1 );
    }
}
