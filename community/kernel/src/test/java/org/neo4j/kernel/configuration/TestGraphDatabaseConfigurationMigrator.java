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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Map;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.LUCENE10;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.SchemaIndex.NATIVE10;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * Test configuration migration rules
 */
public class TestGraphDatabaseConfigurationMigrator
{

    private ConfigurationMigrator migrator;

    @Rule
    public AssertableLogProvider logProvider = new AssertableLogProvider( true );

    @Before
    public void setUp()
    {
        migrator = new GraphDatabaseConfigurationMigrator();
    }

    @Test
    public void testNoMigration()
    {
        assertThat( migrator.apply( stringMap( "foo", "bar" ), NullLog.getInstance() ), equalTo( stringMap( "foo", "bar" ) ) );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void migrateIndexSamplingBufferSizeIfPresent()
    {
        Map<String,String> resultConfig = migrator.apply( stringMap( "dbms.index_sampling.buffer_size", "64m" ), getLog() );
        assertEquals( "Old property should be migrated to new one with correct value",
                resultConfig, stringMap( "dbms.index_sampling.sample_size_limit", "8388608" ));
        assertContainsWarningMessage("dbms.index_sampling.buffer_size has been replaced with dbms.index_sampling.sample_size_limit.");
    }

    @Test
    public void skipMigrationOfIndexSamplingBufferSizeIfNotPresent()
    {
        Map<String,String> resultConfig = migrator.apply( stringMap( "dbms.index_sampling.sample_size_limit", "8388600" ), getLog() );
        assertEquals( "Nothing to migrate should be the same",
                resultConfig, stringMap( "dbms.index_sampling.sample_size_limit", "8388600" ));
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void migrateRestTransactionTimeoutIfPresent()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.transaction_timeout", "120s" ), getLog() );
        assertEquals( "Old property should be migrated to new",
                migratedProperties, stringMap( "dbms.rest.transaction.idle_timeout", "120s" ));

        assertContainsWarningMessage("dbms.transaction_timeout has been replaced with dbms.rest.transaction.idle_timeout.");
    }

    @Test
    public void skipMigrationOfTransactionTimeoutIfNotPresent()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.rest.transaction.idle_timeout", "120s" ), getLog() );
        assertEquals( "Nothing to migrate",
                migratedProperties, stringMap( "dbms.rest.transaction.idle_timeout", "120s" ));
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void migrateExecutionTimeLimitIfPresent()
    {
        Map<String,String> migratedProperties =
                migrator.apply( stringMap( "unsupported.dbms.executiontime_limit.time", "120s" ), getLog() );
        assertEquals( "Old property should be migrated to new",
                migratedProperties, stringMap( "dbms.transaction.timeout", "120s" ));

        assertContainsWarningMessage("unsupported.dbms.executiontime_limit.time has been replaced with dbms.transaction.timeout.");
    }

    @Test
    public void skipMigrationOfExecutionTimeLimitIfNotPresent()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.transaction.timeout", "120s" ), getLog() );
        assertEquals( "Nothing to migrate", migratedProperties, stringMap( "dbms.transaction.timeout", "120s" ));
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void skipMigrationOfExecutionTimeLimitIfTransactionTimeoutConfigured()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "unsupported.dbms.executiontime_limit.time", "12s",
                "dbms.transaction.timeout", "120s" ), getLog() );
        assertEquals( "Should keep pre configured transaction timeout.",
                migratedProperties, stringMap( "dbms.transaction.timeout", "120s" ));
        assertContainsWarningMessage();
    }

    @Test
    public void migrateTransactionEndTimeout()
    {
        Map<String,String> migratedProperties =
                migrator.apply( stringMap( "unsupported.dbms.shutdown_transaction_end_timeout", "12s" ), getLog() );
        assertEquals( "Old property should be migrated to new", migratedProperties,
                stringMap( "dbms.shutdown_transaction_end_timeout", "12s" ) );

        assertContainsWarningMessage( "unsupported.dbms.shutdown_transaction_end_timeout has been " +
                "replaced with dbms.shutdown_transaction_end_timeout." );
    }

    @Test
    public void skipMigrationOfTransactionEndTimeoutIfNotPresent()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.shutdown_transaction_end_timeout", "12s" ), getLog() );
        assertEquals( "Nothing to migrate", migratedProperties, stringMap( "dbms.shutdown_transaction_end_timeout", "12s" ));
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void skipMigrationOfTransactionEndTimeoutIfCustomTransactionEndTimeoutConfigured()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "unsupported.dbms.shutdown_transaction_end_timeout", "12s",
                "dbms.shutdown_transaction_end_timeout", "14s" ), getLog() );
        assertEquals( "Should keep pre configured transaction timeout.",
                migratedProperties, stringMap( "dbms.shutdown_transaction_end_timeout", "14s" ));
        assertContainsWarningMessage();
    }

    @Test
    public void migrateAllowFormatMigration()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.allow_format_migration", "true" ), getLog() );
        assertEquals( "Old property should be migrated to new",
                migratedProperties, stringMap( "dbms.allow_upgrade", "true" ));

        assertContainsWarningMessage("dbms.allow_format_migration has been replaced with dbms.allow_upgrade.");
    }

    @Test
    public void migrateEnableNativeSchemaIndex()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "unsupported.dbms.enable_native_schema_index", "false" ), getLog() );
        assertEquals( "Old property should be migrated to new",
                migratedProperties, stringMap( "dbms.index.default_schema_provider", LUCENE10.providerName() ));

        assertContainsWarningMessage("unsupported.dbms.enable_native_schema_index has been replaced with dbms.index.default_schema_provider.");
    }

    @Test
    public void skipMigrationOfEnableNativeSchemaIndexIfNotPresent()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap( "dbms.index.default_schema_provider", NATIVE10.providerName() ), getLog() );
        assertEquals( "Nothing to migrate", migratedProperties, stringMap( "dbms.index.default_schema_provider", NATIVE10.providerName() ) );
        logProvider.assertNoLoggingOccurred();
    }

    @Test
    public void skipMigrationOfEnableNativeSchemaIndexIfDefaultSchemaIndexConfigured()
    {
        Map<String,String> migratedProperties = migrator.apply( stringMap(
                "dbms.index.default_schema_provider", NATIVE10.providerName(),
                "unsupported.dbms.enable_native_schema_index", "false"
                ), getLog() );
        assertEquals( "Should keep pre configured default schema index.",
                migratedProperties, stringMap( "dbms.index.default_schema_provider", NATIVE10.providerName() ) );
        assertContainsWarningMessage();
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
