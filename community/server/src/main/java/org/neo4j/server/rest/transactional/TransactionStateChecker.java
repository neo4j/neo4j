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

import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.KernelStatement;

public class TransactionStateChecker
{
    private final IsNodeDeletedInCurrentTx nodeCheck;
    private final IsRelationshipDeletedInCurrentTx relCheck;

    TransactionStateChecker( IsNodeDeletedInCurrentTx nodeCheck, IsRelationshipDeletedInCurrentTx relCheck )
    {
        this.nodeCheck = nodeCheck;
        this.relCheck = relCheck;
    }

    public static TransactionStateChecker create( TransitionalPeriodTransactionMessContainer container )
    {
        KernelTransaction topLevelTransactionBoundToThisThread =
                container.getBridge().getTopLevelTransactionBoundToThisThread( true );
        KernelStatement kernelStatement = (KernelStatement) topLevelTransactionBoundToThisThread.acquireStatement();

        return new TransactionStateChecker(
                nodeId -> kernelStatement.hasTxStateWithChanges() &&
                          kernelStatement.txState().nodeIsDeletedInThisTx( nodeId ),
                relId -> kernelStatement.hasTxStateWithChanges() &&
                         kernelStatement.txState().relationshipIsDeletedInThisTx( relId ) );
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
    interface IsNodeDeletedInCurrentTx
    {
        boolean test( long id );
    }

    @FunctionalInterface
    interface IsRelationshipDeletedInCurrentTx
    {
        boolean test( long id );
    }
}
