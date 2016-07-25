/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.server.core;

import org.neo4j.collection.RawIterator;
import org.neo4j.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.api.proc.Neo4jTypes;
import org.neo4j.kernel.api.proc.ProcedureSignature;

import static org.neo4j.kernel.api.proc.ProcedureSignature.procedureSignature;

public class RoleProcedure extends CallableProcedure.BasicProcedure
{
    public static final String NAME = "role";
    private final CoreOrEdge role;

    public RoleProcedure( CoreOrEdge role )
    {
        super( procedureSignature( new ProcedureSignature.ProcedureName( new String[]{"dbms", "cluster"}, NAME ) )
                .out( "role", Neo4jTypes.NTString ).build() );
        this.role = role;
    }

    @Override
    public RawIterator<Object[], ProcedureException> apply( Context ctx, Object[] input ) throws ProcedureException
    {
        return new RawIterator<Object[], ProcedureException>()
        {
            @Override
            public boolean hasNext() throws ProcedureException
            {
                return true;
            }

            @Override
            public Object[] next() throws ProcedureException
            {
                try
                {
                    return new Object[]{role.name()};
                }
                catch ( NullPointerException npe )
                {
                    return new Object[]{"UNKNOWN"};
                }
            }
        };
    }

    public enum CoreOrEdge
    {
        CORE, EDGE
    }
}
