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

import org.neo4j.function.ThrowingFunction;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.Procedure;

/**
 * Tracks components that can be injected into user-defined procedures.
 */
public class ComponentRegistry
{
    private final Map<Class<?>, ThrowingFunction<Procedure.Context, ?, ProcedureException>> suppliers;
    private ThrowingFunction<Class<?>, ThrowingFunction<Procedure.Context, ?, ProcedureException>, ProcedureException> fallback;

    public ComponentRegistry()
    {
        this( new HashMap<>() );
    }

    public ComponentRegistry( Map<Class<?>,ThrowingFunction<Procedure.Context, ?, ProcedureException>> suppliers )
    {
        this.suppliers = suppliers;
    }

    /** Find a supplier for the specified component class, or return null if none exists. */
    public ThrowingFunction<Procedure.Context, ?, ProcedureException> supplierFor( Class<?> type ) throws ProcedureException
    {
        if( suppliers.containsKey( type ))
        {
            return suppliers.get( type );
        }
        else if( fallback != null )
        {
            return fallback.apply( type );
        }
        else
        {
            return null;
        }
    }

    public <T> void register( Class<T> cls, ThrowingFunction<Procedure.Context, T, ProcedureException> supplier )
    {
        suppliers.put( cls, supplier );
    }

    /**
     * If no specific registered component matches, this fallback is triggered, and if it returns a non-null injector, we use that.
     */
    public void registerFallback( ThrowingFunction<Class<?>, ThrowingFunction<Procedure.Context, ?, ProcedureException>, ProcedureException> supplier )
    {
        this.fallback = supplier;
    }
}
