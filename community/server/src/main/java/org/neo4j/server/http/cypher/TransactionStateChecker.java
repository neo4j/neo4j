/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.http.cypher;

import org.neo4j.kernel.impl.api.KernelTransactionImplementation;

public class TransactionStateChecker
{
    private final IsNodeDeletedInCurrentTx nodeCheck;
    private final IsRelationshipDeletedInCurrentTx relCheck;

    public TransactionStateChecker( IsNodeDeletedInCurrentTx nodeCheck, IsRelationshipDeletedInCurrentTx relCheck )
    {
        this.nodeCheck = nodeCheck;
        this.relCheck = relCheck;
    }

    public static TransactionStateChecker create( TransitionalTxManagementKernelTransaction transaction )
    {
        var tx = (KernelTransactionImplementation) transaction.getInternalTransaction().kernelTransaction();
        return new TransactionStateChecker(
                nodeId -> tx.hasTxStateWithChanges() && tx.txState().nodeIsDeletedInThisTx( nodeId ),
                relId -> tx.hasTxStateWithChanges() && tx.txState().relationshipIsDeletedInThisTx( relId ) );
    }

    public boolean isNodeDeletedInCurrentTx( long id )
    {
        return nodeCheck.test( id );
    }

    public boolean isRelationshipDeletedInCurrentTx( long id )
    {
        return relCheck.test( id );
    }

    @FunctionalInterface
    public interface IsNodeDeletedInCurrentTx
    {
        boolean test( long id );
    }

    @FunctionalInterface
    public interface IsRelationshipDeletedInCurrentTx
    {
        boolean test( long id );
    }
}
