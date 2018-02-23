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
package org.neo4j.kernel.configuration;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Test configuration migration rules
 */
public class TestGraphDatabaseConfigurationMigrator
{

    private ConfigurationMigrator migrator;

    @Rule
    public AssertableLogProvider logProvider = new AssertableLogProvider( true );

    @BeforeEach
    void setUp()
    {
        migrator = new GraphDatabaseConfigurationMigrator();
    }

    @Test
    void testNoMigration()
    {
        assertThat( migrator.apply( stringMap( "foo", "bar" ), NullLog.getInstance() ), equalTo( stringMap( "foo", "bar" ) ) );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    void migrateIndexSamplingBufferSizeIfPresent()
    {
        Map<String,String> resultConfig = migrator.apply( stringMap( "dbms.index_sampling.buffer_size", "64m" ), getLog() );
        assertEquals( resultConfig, stringMap( "dbms.index_sampling.sample_size_limit", "8388608" ),
                "Old property should be migrated to new one with correct value" );
        assertContainsWarningMessage("dbms.index_sampling.buffer_size has been replaced with dbms.index_sampling.sample_size_limit.");
    }

    @Test
    void skipMigrationOfIndexSamplingBufferSizeIfNotPresent()
    {
        Map<String,String> resultConfig = migrator.apply( stringMap( "dbms.index_sampling.sample_size_limit", "8388600" ), getLog() );
        assertEquals( resultConfig, stringMap( "dbms.index_sampling.sample_size_limit", "8388600" ),
                "Nothing to migrate should be the same" );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    void migrateRestTransactionTimeoutIfPresent()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.transaction_timeout", "120s" ), getLog() );
        assertEquals( migratedProperties, stringMap( "dbms.rest.transaction.idle_timeout", "120s" ),
                "Old property should be migrated to new" );

        assertContainsWarningMessage("dbms.transaction_timeout has been replaced with dbms.rest.transaction.idle_timeout.");
    }

    @Test
    void skipMigrationOfTransactionTimeoutIfNotPresent()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.rest.transaction.idle_timeout", "120s" ), getLog() );
        assertEquals( migratedProperties, stringMap( "dbms.rest.transaction.idle_timeout", "120s" ),
                "Nothing to migrate" );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    void migrateExecutionTimeLimitIfPresent()
    {
        Map<String,String> migratedProperties =
                migrator.apply( stringMap( "unsupported.dbms.executiontime_limit.time", "120s" ), getLog() );
        assertEquals( migratedProperties, stringMap( "dbms.transaction.timeout", "120s" ),
                "Old property should be migrated to new" );

        assertContainsWarningMessage("unsupported.dbms.executiontime_limit.time has been replaced with dbms.transaction.timeout.");
    }

    @Test
    void skipMigrationOfExecutionTimeLimitIfNotPresent()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.transaction.timeout", "120s" ), getLog() );
        assertEquals( migratedProperties, stringMap( "dbms.transaction.timeout", "120s" ), "Nothing to migrate" );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    void skipMigrationOfExecutionTimeLimitIfTransactionTimeoutConfigured()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "unsupported.dbms.executiontime_limit.time", "12s",
                "dbms.transaction.timeout", "120s" ), getLog() );
        assertEquals( migratedProperties, stringMap( "dbms.transaction.timeout", "120s" ),
                "Should keep pre configured transaction timeout." );
        assertContainsWarningMessage();
    }

    @Test
    void migrateTransactionEndTimeout()
    {
        Map<String,String> migratedProperties =
                migrator.apply( stringMap( "unsupported.dbms.shutdown_transaction_end_timeout", "12s" ), getLog() );
        assertEquals( migratedProperties, stringMap( "dbms.shutdown_transaction_end_timeout", "12s" ),
                "Old property should be migrated to new" );

        assertContainsWarningMessage( "unsupported.dbms.shutdown_transaction_end_timeout has been " +
                "replaced with dbms.shutdown_transaction_end_timeout." );
    }

    @Test
    void skipMigrationOfTransactionEndTimeoutIfNotPresent()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.shutdown_transaction_end_timeout", "12s" ), getLog() );
        assertEquals( migratedProperties, stringMap( "dbms.shutdown_transaction_end_timeout", "12s" ),
                "Nothing to migrate" );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    void skipMigrationOfTransactionEndTimeoutIfCustomTransactionEndTimeoutConfigured()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "unsupported.dbms.shutdown_transaction_end_timeout", "12s",
                "dbms.shutdown_transaction_end_timeout", "14s" ), getLog() );
        assertEquals( migratedProperties, stringMap( "dbms.shutdown_transaction_end_timeout", "14s" ),
                "Should keep pre configured transaction timeout." );
        assertContainsWarningMessage();
    }

    @Test
    void migrateAllowFormatMigration()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.allow_format_migration", "true" ), getLog() );
        assertEquals( migratedProperties, stringMap( "dbms.allow_upgrade", "true" ),
                "Old property should be migrated to new" );

        assertContainsWarningMessage("dbms.allow_format_migration has been replaced with dbms.allow_upgrade.");
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
