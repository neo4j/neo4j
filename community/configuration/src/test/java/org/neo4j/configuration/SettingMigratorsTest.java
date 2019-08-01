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

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.logging.AssertableLogProvider;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.SettingValueParsers.FALSE;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.logging.AssertableLogProvider.inLog;

class SettingMigratorsTest
{
    @Test
    void shouldRemoveAllowKeyGenerationFrom35ConfigFormat() throws Throwable
    {
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.default.allow_key_generation", TRUE );
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.default.allow_key_generation", FALSE );
    }

    @Test
    void shouldRemoveAllowKeyGeneration() throws Throwable
    {
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.pem.default.allow_key_generation", TRUE );
        shouldRemoveAllowKeyGeneration( "dbms.ssl.policy.pem.default.allow_key_generation", FALSE );
    }

    private void shouldRemoveAllowKeyGeneration( String toRemove, String value )
    {
        var config = Config.newBuilder().setRaw( Map.of( toRemove, value ) ).build();

        var logProvider = new AssertableLogProvider();
        config.setLogger( logProvider.getLog( Config.class ) );

        assertThrows( IllegalArgumentException.class, () -> config.getSetting( toRemove ) );

        logProvider.assertAtLeastOnce( inLog( Config.class ).warn( "Setting %s is removed. A valid key and certificate are required " +
                "to be present in the key and certificate path configured in this ssl policy.", toRemove ) );
    }
}
