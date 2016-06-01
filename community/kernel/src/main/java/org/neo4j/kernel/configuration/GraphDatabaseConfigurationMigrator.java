/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
                "dbms.index_sampling.buffer_size has been replaced with dbms.index_sampling.sample_size_limit" )
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
    }
}
