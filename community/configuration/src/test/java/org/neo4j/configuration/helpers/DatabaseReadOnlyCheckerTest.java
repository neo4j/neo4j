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
package org.neo4j.configuration.helpers;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.read_only_database_default;
import static org.neo4j.configuration.GraphDatabaseSettings.writable_databases;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.readOnly;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;

class DatabaseReadOnlyCheckerTest
{
    @Test
    void readOnlyCheckerThrowsExceptionOnCheck()
    {
        var e = assertThrows( Exception.class, () -> readOnly().check() );
        assertThat( e ).hasRootCauseInstanceOf( ReadOnlyDbException.class );
    }

    @Test
    void writeOnlyDoesNotThrowExceptionOnCheck()
    {
        assertDoesNotThrow( () -> writable().check() );
    }

    @Test
    void byDefaultDatabaseAreWritable()
    {
        var config = Config.defaults();
        var checker = new DatabaseReadOnlyChecker.Default( config, "foo" );
        assertFalse( checker.isReadOnly() );
        assertDoesNotThrow( checker::check );
    }

    @Test
    void readOnlyDatabase()
    {
        var dbName = "foo";
        var config = Config.defaults( GraphDatabaseSettings.read_only_databases, Set.of( dbName ) );
        var checker = new DatabaseReadOnlyChecker.Default( config, dbName );
        var e = assertThrows( Exception.class, checker::check );
        assertThat( e ).hasRootCauseInstanceOf( ReadOnlyDbException.class );
    }

    @Test
    void writableDatabase()
    {
        var dbName = "foo";
        var config = Config.defaults( Map.of( writable_databases, Set.of( dbName ), read_only_database_default, true ) );
        var checker = new DatabaseReadOnlyChecker.Default( config, dbName );
        assertFalse( checker.isReadOnly() );
        assertDoesNotThrow( checker::check );
    }
}
