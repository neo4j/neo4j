/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.LifecycleOperations;
import org.neo4j.kernel.api.StatementContextParts;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.api.operations.EntityReadOperations;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.KeyReadOperations;
import org.neo4j.kernel.api.operations.KeyWriteOperations;
import org.neo4j.kernel.api.operations.SchemaReadOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.operations.SchemaWriteOperations;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class StatementContextTestHelper
{
    public static StatementContextParts mockedParts()
    {
        StatementContextParts stmtContextParts = new StatementContextParts(
            mock( KeyReadOperations.class ),
            mock( KeyWriteOperations.class ),
            mock( EntityReadOperations.class ),
            mock( EntityWriteOperations.class ),
            mock( SchemaReadOperations.class ),
            mock( SchemaWriteOperations.class ),
            mock( SchemaStateOperations.class ),
            mock( LifecycleOperations.class ) );
        return stmtContextParts;
    }
    
    public static StatementContextParts mockedParts( TransactionContext txContext )
    {
        StatementContextParts mock = mockedParts();
        when( txContext.newStatementContext() ).thenReturn( mock );
        return mock;
    }
    
    private StatementContextTestHelper()
    {   // Singleton
    }
}
