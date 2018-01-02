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
package org.neo4j.kernel.impl.query;

import java.util.HashMap;
import java.util.Map;

/**
 * A unique QuerySession should be created and provided to
 * {@link QueryExecutionEngine#executeQuery(String, java.util.Map, QuerySession)} for each cypher query.
 * If queryLogging is enabled {@link org.neo4j.graphdb.factory.GraphDatabaseSettings#log_queries},
 * the result of {@link #toString()} will be serialized in the log.
 */
public abstract class QuerySession
{
    private final Map<MetadataKey<?>, Object> metadata = new HashMap<>();

    /**
     * Defines what the output of this QuerySession should be.
     *
     * @return The output of this QuerySession.
     */
    @Override
    public abstract String toString();

    public final <ValueType> ValueType put( MetadataKey<ValueType> key, ValueType value )
    {
        return key.valueType.cast( metadata.put( key, value ) );
    }

    public final <ValueType> ValueType get( MetadataKey<ValueType> key )
    {
        return key.valueType.cast( metadata.get( key ) );
    }

    public final <ValueType> ValueType remove( MetadataKey<ValueType> key )
    {
        return key.valueType.cast( metadata.remove( key ) );
    }

    public static final class MetadataKey<ValueType>
    {
        private final Class<ValueType> valueType;
        private final String name;

        public MetadataKey( Class<ValueType> valueType, String name )
        {
            this.valueType = valueType;
            this.name = name;
        }

        @Override
        public String toString()
        {
            return String.format( "QuerySession.MetadataKey(%s:%s)", name, valueType.getName() );
        }
    }
}
