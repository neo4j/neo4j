/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.DataWriteOperations;
import org.neo4j.kernel.api.ExecutionStatisticsOperations;
import org.neo4j.kernel.api.ProcedureCallOperations;
import org.neo4j.kernel.api.QueryRegistryOperations;
import org.neo4j.kernel.api.ReadOperations;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;

public class StubStatement implements Statement
{
    private final ReadOperations readOperations;

    StubStatement( ReadOperations readOperations )
    {
        this.readOperations = readOperations;
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public ReadOperations readOperations()
    {
        return readOperations;
    }

    @Override
    public TokenWriteOperations tokenWriteOperations()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public DataWriteOperations dataWriteOperations() throws InvalidTransactionTypeKernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public SchemaWriteOperations schemaWriteOperations() throws InvalidTransactionTypeKernelException
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public QueryRegistryOperations queryRegistration()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public ProcedureCallOperations procedureCallOperations()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public ExecutionStatisticsOperations executionStatisticsOperations()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
