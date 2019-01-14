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
package org.neo4j.kernel.impl.api;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Used for the actual storage of "schema state".
 * Schema state is transient state that should be invalidated when the schema changes.
 * Examples of things stored in schema state is execution plans for cypher.
 */
public class DatabaseSchemaState implements SchemaState
{
    private Map<Object, Object> state;
    private final Log log;

    public DatabaseSchemaState( LogProvider logProvider )
    {
        this.state = new ConcurrentHashMap<>(  );
        this.log = logProvider.getLog( getClass() );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <K, V> V get( K key )
    {
        return (V) state.get( key );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    public <K, V> V getOrCreate( K key, Function<K,V> creator )
    {
        return (V) state.computeIfAbsent( key, (Function<Object, Object>) creator );
    }

    @Override
    public <K, V> void put( K key, V value )
    {
        state.put( key, value );
    }

    @Override
    public void clear()
    {
        state.clear();
        log.debug( "Schema state store has been cleared." );
    }
}
