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

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

class TestGraphDatabaseConfigurationMigrator
{
    private final ConfigurationMigrator migrator = new GraphDatabaseConfigurationMigrator();

    private AssertableLogProvider logProvider;

    @BeforeEach
    void setUp()
    {
        logProvider = new AssertableLogProvider( true );
    }

    @Test
    void testNoMigration()
    {
        assertThat( migrator.apply( stringMap( "foo", "bar" ), NullLog.getInstance() ), equalTo( stringMap( "foo", "bar" ) ) );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    void migrateOldTransactionLogsDirectories()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.directories.tx_log", "C:/" ), getLog() );
        assertEquals( migratedProperties, Collections.emptyMap() );

        assertContainsWarningMessage("WARNING! Deprecated configuration options used. See manual for details");
        assertContainsWarningMessage("dbms.directories.tx_log is not supported anymore. " +
                "Please use dbms.directories.transaction.logs.root to set root directory for databases transaction logs. " +
                "Each individual database will place its logs into a separate subdirectory under configured root.");
    }

    private Log getLog()
    {
        return logProvider.getLog( GraphDatabaseConfigurationMigrator.class );
    }

    private void assertContainsWarningMessage()
    {
        logProvider.assertContainsMessageContaining( "WARNING! Deprecated configuration options used. See manual for details" );
    }

    private void assertContainsWarningMessage( String deprecationMessage )
    {
        assertContainsWarningMessage();
        if ( StringUtils.isNotEmpty( deprecationMessage ) )
        {
            logProvider.assertContainsMessageContaining( deprecationMessage );
        }
    }
}
