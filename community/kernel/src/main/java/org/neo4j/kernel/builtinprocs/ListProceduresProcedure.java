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

import java.util.ArrayList;
import java.util.Set;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.Procedure;
import org.neo4j.kernel.api.proc.ProcedureSignature;

import static org.neo4j.helpers.collection.Iterables.asRawIterator;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.kernel.api.ReadOperations.readOperations;
import static org.neo4j.kernel.api.proc.Neo4jTypes.NTString;

public class ListProceduresProcedure extends Procedure.BasicProcedure
{
    public ListProceduresProcedure( ProcedureSignature.ProcedureName procedureName )
    {
        super( ProcedureSignature.procedureSignature( procedureName )
                .out( "name", NTString )
                .out( "signature", NTString )
                .build() );
    }

    @Override
    public RawIterator<Object[],ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        Set<ProcedureSignature> procedureSignatures = ctx.get( readOperations ).proceduresGetAll();
        ArrayList<ProcedureSignature> sorted = new ArrayList<>( procedureSignatures );
        sorted.sort( (a,b) -> a.name().toString().compareTo( b.name().toString() ) );

        RawIterator<ProcedureSignature,ProcedureException> signatures = asRawIterator( sorted.iterator() );
        return map(  ( signature ) -> new Object[]{ signature.name().toString(), signature.toString() }, signatures );
    }
}
