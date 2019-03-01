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
package org.neo4j.kernel.diagnostics.providers;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static org.neo4j.configuration.GraphDatabaseSettings.default_database;
import static org.neo4j.configuration.GraphDatabaseSettings.max_concurrent_transactions;

class ConfigDiagnosticsTest
{
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final Log log = logProvider.getLog( ConfigDiagnostics.class );

    @Test
    void dumpConfigValues()
    {
        Config config = Config.builder().withSetting( default_database, "testDb" ).withSetting( max_concurrent_transactions, "400" ).build();

        ConfigDiagnostics configDiagnostics = new ConfigDiagnostics( config );
        configDiagnostics.dump( log.infoLogger() );

        logProvider.assertLogStringContains( "DBMS provided settings:" );
        logProvider.assertLogStringContains( max_concurrent_transactions.name() + "=400" );
        logProvider.assertLogStringContains( default_database.name() + "=testDb" );
        logProvider.assertNoMessagesContaining( "No provided DBMS settings." );
    }

    @Test
    void dumpDefaultConfig()
    {
        Config config = Config.defaults();

        ConfigDiagnostics configDiagnostics = new ConfigDiagnostics( config );
        configDiagnostics.dump( log.infoLogger() );

        logProvider.assertLogStringContains( "No provided DBMS settings." );
        logProvider.assertNoMessagesContaining( "DBMS provided settings" );
    }
}
