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

import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.ByteUnit;

/**
 * Migrations of old graph database settings.
 */
public class GraphDatabaseConfigurationMigrator extends BaseConfigurationMigrator
{
    public GraphDatabaseConfigurationMigrator()
    {
        registerMigrations();
    }

    private void registerMigrations()
    {
        add( new SpecificPropertyMigration( "dbms.index_sampling.buffer_size",
                "dbms.index_sampling.buffer_size has been replaced with dbms.index_sampling.sample_size_limit." )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
            {
                if ( StringUtils.isNotEmpty( value ) )
                {
                    String oldSettingDefaultValue = GraphDatabaseSettings.index_sampling_buffer_size.getDefaultValue();
                    Long newValue = oldSettingDefaultValue.equals( value ) ? ByteUnit.mebiBytes( 8 )
                                                                           : Settings.BYTES.apply( value );
                    rawConfiguration.put( "dbms.index_sampling.sample_size_limit", String.valueOf( newValue ) );
                }
            }
        } );

        add( new SpecificPropertyMigration( "dbms.transaction_timeout",
                "dbms.transaction_timeout has been replaced with dbms.rest.transaction.idle_timeout." )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
            {
                rawConfiguration.put( "dbms.rest.transaction.idle_timeout", value );
            }
        } );

        add( new SpecificPropertyMigration( "unsupported.dbms.executiontime_limit.enabled",
                "unsupported.dbms.executiontime_limit.enabled is not supported anymore. " +
                "Set dbms.transaction.timeout settings to some positive value to enable execution guard and set " +
                "transaction timeout." )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
            {
            }
        } );

        add( new SpecificPropertyMigration( "unsupported.dbms.executiontime_limit.time",
                "unsupported.dbms.executiontime_limit.time has been replaced with dbms.transaction.timeout." )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
            {
                if ( StringUtils.isNotEmpty( value ) )
                {
                    rawConfiguration.putIfAbsent( GraphDatabaseSettings.transaction_timeout.name(), value );
                }
            }
        } );
        add( new SpecificPropertyMigration( "unsupported.dbms.shutdown_transaction_end_timeout",
                "unsupported.dbms.shutdown_transaction_end_timeout has been " +
                        "replaced with dbms.shutdown_transaction_end_timeout." )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
            {
                if ( StringUtils.isNotEmpty( value ) )
                {
                    rawConfiguration.putIfAbsent( GraphDatabaseSettings.shutdown_transaction_end_timeout.name(), value );
                }
            }
        } );
        add( new SpecificPropertyMigration( "dbms.allow_format_migration",
                "dbms.allow_format_migration has been replaced with dbms.allow_upgrade." )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
            {
                rawConfiguration.put( GraphDatabaseSettings.allow_upgrade.name(), value );
            }
        } );
        add( new SpecificPropertyMigration( "dbms.logs.timezone",
                "dbms.logs.timezone has been replaced with dbms.db.timezone." )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
            {
                rawConfiguration.put( GraphDatabaseSettings.db_timezone.name(), value );
            }
        } );
        add( new SpecificPropertyMigration( "unsupported.dbms.enable_native_schema_index",
                "unsupported.dbms.enable_native_schema_index has been replaced with dbms.index.default_schema_provider." )
        {
            @Override
            public void setValueWithOldSetting( String value, Map<String,String> rawConfiguration )
            {
                if ( value.equals( Settings.FALSE ) )
                {
                    rawConfiguration.putIfAbsent( GraphDatabaseSettings.default_schema_provider.name(),
                            GraphDatabaseSettings.SchemaIndex.LUCENE10.providerName() );
                }
            }
        } );
    }
}
