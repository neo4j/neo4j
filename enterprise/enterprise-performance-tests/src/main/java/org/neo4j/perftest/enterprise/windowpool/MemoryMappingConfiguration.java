/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.perftest.enterprise.windowpool;

import java.util.Map;

import org.neo4j.helpers.Settings;

public class MemoryMappingConfiguration
{
    public static void addLegacyMemoryMappingConfiguration( Map<String, String> config, String totalMappedMemory )
    {
        long mappedMemory = Settings.BYTES.apply( totalMappedMemory );
        long memoryUnit = mappedMemory / 472;

        config.put( "neostore.nodestore.db.mapped_memory", mega( 20 * memoryUnit ) );
        config.put( "neostore.propertystore.db.mapped_memory", mega( 90 * memoryUnit ) );
        config.put( "neostore.propertystore.db.index.mapped_memory", mega( 1 * memoryUnit ) );
        config.put( "neostore.propertystore.db.index.keys.mapped_memory", mega( 1 * memoryUnit ) );
        config.put( "neostore.propertystore.db.strings.mapped_memory", mega( 130 * memoryUnit ) );
        config.put( "neostore.propertystore.db.arrays.mapped_memory", mega( 130 * memoryUnit ) );
        config.put( "neostore.relationshipstore.db.mapped_memory", mega( 100 * memoryUnit ) );
    }

    private static String mega( long bytes )
    {
        return (bytes / 1024 / 1024) + "M";
    }
}
