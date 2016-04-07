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
package org.neo4j.kernel.builtinprocs;

import java.io.IOException;
import java.util.Collections;

import org.neo4j.collection.RawIterator;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.ProcedureSignature;
import org.neo4j.kernel.api.proc.ProcedureSignature.ProcedureName;
import org.neo4j.kernel.api.security.AccessMode;
import org.neo4j.server.security.auth.AuthSubject;
import org.neo4j.server.security.auth.exception.IllegalCredentialsException;

import static org.neo4j.helpers.collection.Iterators.asRawIterator;
import static org.neo4j.helpers.collection.Iterators.map;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTString;
import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

/**
 * This procedure changes the existing password to the supplied password for
 * the user identified by the supplied username.
 */
public class AlterUserPasswordProcedure extends CallableProcedure.BasicProcedure
{
    public AlterUserPasswordProcedure( ProcedureName name )
    {
        super( procedureSignature( name )
                .in( "password", NTString )
                .mode( ProcedureSignature.Mode.DBMS )
                .build() );
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        AccessMode accessMode = ctx.get( Context.ACCESS_MODE );
        if ( !(accessMode instanceof AuthSubject) )
        {
            throw new AuthorizationViolationException( "Invalid attempt to change the password" );
        }
        AuthSubject authSubject = (AuthSubject) accessMode;

        try
        {
            authSubject.setPassword( input[0].toString() );
            return map( ( l ) -> new Object[]{l}, asRawIterator( Collections.emptyIterator() ) );
        }
        catch ( IOException e )
        {
            throw new ProcedureException( Status.Security.Forbidden, e,
                    "Failed to change the password for the provided username" );
        }
        catch ( IllegalCredentialsException e )
        {
           throw new ProcedureException( e.status(), e, e.getMessage() );
        }
    }
}
