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
package org.neo4j.kernel.impl.api;

import org.neo4j.function.Function;
import org.neo4j.kernel.impl.api.operations.SchemaStateOperations;

public class SchemaStateConcern implements SchemaStateOperations
{
    private final UpdateableSchemaState schemaState;

    public SchemaStateConcern( UpdateableSchemaState schemaState )
    {
        this.schemaState = schemaState;
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( KernelStatement state, K key, Function<K, V> creator )
    {
        return schemaState.getOrCreate( key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( KernelStatement state, K key )
    {
        return schemaState.get( key ) != null;
    }

    @Override
    public void schemaStateFlush( KernelStatement state )
    {
        schemaState.clear();
    }
}
