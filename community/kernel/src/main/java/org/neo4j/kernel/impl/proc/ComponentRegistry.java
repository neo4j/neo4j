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
package org.neo4j.kernel.impl.proc;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.kernel.api.proc.Procedure;

/**
 * Tracks components that can be injected into user-defined procedures.
 */
public class ComponentRegistry
{
    private final Map<Class<?>, Function<Procedure.Context, ?>> suppliers;

    public ComponentRegistry()
    {
        this( new HashMap<>() );
    }

    public ComponentRegistry( Map<Class<?>,Function<Procedure.Context,?>> suppliers )
    {
        this.suppliers = suppliers;
    }

    public Function<Procedure.Context,?> supplierFor( Class<?> type )
    {
        return suppliers.get( type );
    }

    public <T> void register( Class<T> cls, Function<Procedure.Context,T> supplier )
    {
        suppliers.put( cls, supplier );
    }
}
