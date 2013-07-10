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
package org.neo4j.server.rest.transactional;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.impl.transaction.TxManager;

public class TransitionalPeriodTransactionMessContainer implements KernelAPI
{
    private final GraphDatabaseAPI db;
    private final TxManager txManager;

    public TransitionalPeriodTransactionMessContainer( GraphDatabaseAPI db )
    {
        this.db = db;
        this.txManager = db.getDependencyResolver().resolveDependency( TxManager.class );
    }

    @Override
    public KernelTransaction newTransaction()
    {
        db.beginTx();
        
        // Get and use the TransactionContext created in db.beginTx(). The role of creating
        // TransactionContexts will be reversed soonish.
        return new TransitionalTxManagementKernelTransaction( txManager.getKernelTransaction(), txManager );
    }

    @Override
    public StatementOperationParts readOnlyStatementOperations()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public StatementOperationParts statementOperations()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void bootstrapAfterRecovery()
    {
    }
}
