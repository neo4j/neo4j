/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api;

import java.util.HashMap;
import java.util.Map;

public abstract class IndexUsage
{
    public static IndexUsage schemaIndexUsage( String identifier, String label, String propertyKey )
    {
        return new SchemaIndexUsage( identifier, label, propertyKey );
    }

    public static IndexUsage legacyIndexUsage( String identifier, String entityType, String index )
    {
        return new LegacyIndexUsage( identifier, index, entityType );
    }

    abstract Map<String,String> asMap();

    final String identifier;

    private IndexUsage( String identifier )
    {
        this.identifier = identifier;
    }

    private static class SchemaIndexUsage extends IndexUsage
    {
        private final String label;
        private final String propertyKey;

        private SchemaIndexUsage( String identifier, String label, String propertyKey )
        {
            super( identifier );
            this.label = label;
            this.propertyKey = propertyKey;
        }

        Map<String,String> asMap()
        {
            Map<String,String> map = new HashMap<>();
            map.put( "indexType", "SCHEMA INDEX" );
            map.put( "entityType", "NODE" );
            map.put( "identifier", identifier );
            map.put( "label", label );
            map.put( "propertyKey", propertyKey );
            return map;
        }
    }

    private static class LegacyIndexUsage extends IndexUsage
    {
        private final String index;
        private final String entityType;

        private LegacyIndexUsage( String identifier, String index, String entityType )
        {
            super( identifier );
            this.index = index;
            this.entityType = entityType;
        }

        @Override
        Map<String,String> asMap()
        {
            Map<String,String> map = new HashMap<>();
            map.put( "indexType", "LEGACY INDEX" );
            map.put( "entityType", entityType );
            map.put( "identifier", identifier );
            map.put( "indexName", index );
            return map;
        }
    }
}
