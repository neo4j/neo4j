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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.neo4j.kernel.api.exceptions.WriteOnReadOnlyAccessDbException;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atMostOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;

class DatabaseReadOnlyCheckerTest
{
    @Test
    void readOnlyCheckerThrowsExceptionOnCheck()
    {
        var e = assertThrows( Exception.class, () -> readOnly().check() );
        assertThat( e ).hasRootCauseInstanceOf( WriteOnReadOnlyAccessDbException.class );
    }

    @Test
    void writeOnlyDoesNotThrowExceptionOnCheck()
    {
        assertDoesNotThrow( () -> writable().check() );
    }

    @Test
    void databaseCheckersShouldReflectUpdatesToGlobalChecker()
    {
        var foo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var bar = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
        var databases = new HashSet<NamedDatabaseId>();
        databases.add( foo );
        var globalChecker = new ReadOnlyDatabases( () -> {
            var snapshot = Set.copyOf( databases );
            return snapshot::contains;
        } );
        var fooChecker = globalChecker.forDatabase( foo );
        var barChecker = globalChecker.forDatabase( bar );

        assertTrue( fooChecker.isReadOnly() );
        assertFalse( barChecker.isReadOnly() );

        databases.add( bar );
        globalChecker.refresh();
        assertTrue( barChecker.isReadOnly() );
    }

    @Test
    void databaseCheckerShouldCacheLookupsFromGlobalChecker()
    {
        var foo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var bar = DatabaseIdFactory.from( "bar", UUID.randomUUID() );
        var databases = new HashSet<NamedDatabaseId>();
        databases.add( foo );
        var globalChecker = spy( new ReadOnlyDatabases( () -> {
            var snapshot = Set.copyOf( databases );
            return snapshot::contains;
        } ) );
        var fooChecker = globalChecker.forDatabase( foo );
        var barChecker = globalChecker.forDatabase( bar );

        // when
        assertTrue( fooChecker.isReadOnly() );
        assertTrue( fooChecker.isReadOnly() );

        // then
        verify( globalChecker, atMostOnce() ).isReadOnly( foo );

        // when
        databases.add( bar );
        globalChecker.refresh();

        assertTrue( barChecker.isReadOnly() );
        assertTrue( fooChecker.isReadOnly() );

        // then
        verify( globalChecker, times( 2 ) ).isReadOnly( foo );
    }
}
