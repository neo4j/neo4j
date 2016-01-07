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

import java.util.stream.Stream;

import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.proc.Procedure;
import org.neo4j.proc.ProcedureSignature;
import org.neo4j.proc.ProcedureSignature.ProcedureName;

import static org.neo4j.helpers.collection.IteratorUtil.stream;
import static org.neo4j.kernel.api.ProcedureRead.readStatement;

public class ListLabelsProcedure extends Procedure.BasicProcedure
{
    public ListLabelsProcedure( ProcedureName name )
    {
        super(new ProcedureSignature( name ));
    }

    @Override
    public Stream<Object[]> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        return stream( ctx.get( readStatement ).labelsGetAllTokens() ).map( ( token) -> new Object[]{ token.name() } );
    }
}
