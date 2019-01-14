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
package org.neo4j.kernel.api.proc;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Not thread safe. Basic context backed by a map.
 */
public class BasicContext implements Context
{
    private final Map<String, Object> values = new HashMap<>();

    @Override
    public <T> T get( Key<T> key ) throws ProcedureException
    {
        Object o = values.get( key.name() );
        if ( o == null )
        {
            throw new ProcedureException( Status.Procedure.ProcedureCallFailed,
                    "There is no `%s` in the current procedure call context.", key.name() );
        }
        return (T) o;
    }

    @Override
    public <T> T getOrElse( Key<T> key, T orElse )
    {
        Object o = values.get( key.name() );
        if ( o == null )
        {
            return orElse;
        }
        return (T) o;
    }

    public <T> void put( Key<T> key, T value )
    {
        values.put( key.name(), value );
    }
}
