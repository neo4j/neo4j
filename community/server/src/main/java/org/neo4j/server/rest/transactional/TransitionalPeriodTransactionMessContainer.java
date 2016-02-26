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
package org.neo4j.server.rest.transactional;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.AccessMode;
import org.neo4j.kernel.api.KernelTransaction.Type;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class TransitionalPeriodTransactionMessContainer
{
    private final GraphDatabaseFacade db;
    private final ThreadToStatementContextBridge txBridge;

    public TransitionalPeriodTransactionMessContainer( GraphDatabaseFacade db )
    {
        this.db = db;
        this.txBridge = db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );
    }

    public TransitionalTxManagementKernelTransaction newTransaction( boolean implicitTransaction, AccessMode mode )
    {
        Type type = implicitTransaction ? Type.implicit : Type.explicit;
        Transaction tx = db.beginTransaction( type, mode );
        return new TransitionalTxManagementKernelTransaction( new TransactionTerminator( tx ), txBridge );
    }

    public ThreadToStatementContextBridge getBridge()
    {
        return txBridge;
    }

}
