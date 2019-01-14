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
package org.neo4j.kernel.impl.store.format;

import java.util.Map;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_BLOCK_SIZE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_LABEL_BLOCK_SIZE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.MINIMAL_BLOCK_SIZE;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.array_block_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.label_block_size;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.string_block_size;

/**
 * There are couple of configuration options that should be adapted for each particular implementation of record format.
 * In case if user already set value of those properties we will keep them, otherwise format specific value will be
 * evaluated and configuration will be adapted.
 */
public class RecordFormatPropertyConfigurator
{
    private final RecordFormats recordFormats;
    private final Config config;

    public RecordFormatPropertyConfigurator( RecordFormats recordFormats, Config config )
    {
        this.recordFormats = recordFormats;
        this.config = config;
    }

    private static void configureIntegerSetting( Config config, Setting<Integer> setting,
            int fullBlockSize, int headerSize, Map<String,String> formatConfig )
    {
        Integer defaultValue = Integer.parseInt( setting.getDefaultValue() );
        int propertyValue = config.get( setting );
        if ( propertyValue == defaultValue )
        {
            int updatedBlockSize = fullBlockSize - headerSize;
            if ( updatedBlockSize != propertyValue )
            {
                if ( updatedBlockSize < MINIMAL_BLOCK_SIZE )
                {
                    throw new IllegalArgumentException( "Block size should be bigger then " + MINIMAL_BLOCK_SIZE );
                }
                addFormatSetting( formatConfig, setting, updatedBlockSize );
            }
        }
    }

    private static void addFormatSetting( Map<String,String> configMap, Setting setting, int value )
    {
        configMap.put( setting.name(), String.valueOf( value ) );
    }

    public void configure()
    {
        Map<String,String> formatConfig = MapUtil.stringMap();
        int headerSize = recordFormats.dynamic().getRecordHeaderSize();

        configureIntegerSetting( config, string_block_size, DEFAULT_BLOCK_SIZE, headerSize, formatConfig );
        configureIntegerSetting( config, array_block_size, DEFAULT_BLOCK_SIZE, headerSize, formatConfig );
        configureIntegerSetting( config, label_block_size, DEFAULT_LABEL_BLOCK_SIZE, headerSize, formatConfig );
        if ( !formatConfig.isEmpty() )
        {
            config.augment( formatConfig );
        }
    }
}
