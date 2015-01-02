/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api;

import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.operations.LegacyKernelOperations;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.persistence.PersistenceManager.ResourceHolder;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KernelTransactionFactory
{
    static KernelTransaction kernelTransaction()
    {
        PersistenceManager pm = mock( PersistenceManager.class );
        when( pm.getResource() ).thenReturn( mock( ResourceHolder.class ) );
        return new KernelTransactionImplementation( mock( StatementOperationParts.class ),
                mock( LegacyKernelOperations.class ) , false, mock( SchemaWriteGuard.class ), null, null,
                mock( AbstractTransactionManager.class ), null, null, null, pm,
                null, mock( NeoStore.class ), null );
    }
}
