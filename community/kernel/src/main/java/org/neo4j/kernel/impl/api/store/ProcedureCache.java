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
package org.neo4j.kernel.impl.api.store;

import java.util.Iterator;
import java.util.Map;

import org.neo4j.kernel.api.procedures.ProcedureDescriptor;
import org.neo4j.kernel.api.procedures.ProcedureSignature;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

/**
 * A temporary in-memory storage for procedures, until we build "real" storage for them in 3.0.
 * TODO!
 */
public class ProcedureCache
{
    private final Map<ProcedureSignature.ProcedureName, ProcedureDescriptor> procedures = new CopyOnWriteHashMap<>();

    public Iterator<ProcedureDescriptor> getAll()
    {
        return procedures.values().iterator();
    }

    public ProcedureDescriptor get( ProcedureSignature.ProcedureName name )
    {
        return procedures.get( name );
    }

    // TODO: This is temporary, to be replaced by the regular tx log/store layer stuff
    public synchronized void createProcedure( ProcedureDescriptor descriptor )
    {
        assert !procedures.containsKey( descriptor.signature().name() );
        procedures.put( descriptor.signature().name(), descriptor );
    }

    public synchronized void dropProcedure( ProcedureDescriptor procedureDescriptor )
    {
        assert procedures.containsKey( procedureDescriptor.signature().name() );
        procedures.remove( procedureDescriptor.signature().name() );
    }
}
