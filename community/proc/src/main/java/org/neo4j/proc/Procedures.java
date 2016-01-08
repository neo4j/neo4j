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
package org.neo4j.proc;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;


public class Procedures
{
    private final Map<ProcedureSignature.ProcedureName,Procedure> procedures = new HashMap<>();

    /**
     * Register a new procedure. This method must not be called concurrently with {@link #get(ProcedureSignature.ProcedureName)}.
     * @param proc the procedure.
     */
    public synchronized void register( Procedure proc ) throws ProcedureException
    {
        ProcedureSignature signature = proc.signature();
        ProcedureSignature.ProcedureName name = signature.name();
        if( procedures.putIfAbsent( name, proc ) != null )
        {
            throw new ProcedureException( Status.Procedure.FailedRegistration, "Unable to register procedure, because the name `%s` is already in use.", name );
        }
    }

    public ProcedureSignature get( ProcedureSignature.ProcedureName name ) throws ProcedureException
    {
        Procedure proc = procedures.get( name );
        if( proc == null )
        {
            throw noSuchProcedure( name );
        }
        return proc.signature();
    }

    public Stream<Object[]> call( ProcedureSignature.ProcedureName name, Object[] input ) throws ProcedureException
    {
        Procedure proc = procedures.get( name );
        if( proc == null )
        {
            throw noSuchProcedure( name );
        }
        return proc.apply( input );
    }

    private ProcedureException noSuchProcedure( ProcedureSignature.ProcedureName name )
    {
        return new ProcedureException( Status.Procedure.NoSuchProcedure, "There is no procedure with the name `%s` registered for this database instance. " +
                                                                        "Please ensure you've spelled the procedure name correctly and that the " +
                                                                        "procedure is properly deployed.", name );
    }
}
