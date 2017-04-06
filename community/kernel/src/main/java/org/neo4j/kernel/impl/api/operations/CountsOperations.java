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
package org.neo4j.kernel.impl.api.operations;

import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.impl.api.KernelStatement;
import org.neo4j.register.Register.DoubleLongRegister;

public interface CountsOperations
{
    /** @see org.neo4j.kernel.api.CountsRead#countsForNode(int) */
    long countsForNode( KernelStatement statement, int labelId );

    /** @see org.neo4j.kernel.api.CountsRead#countsForNodeWithoutTxState(int) */
    long countsForNodeWithoutTxState( KernelStatement statement, int labelId );

    /** @see org.neo4j.kernel.api.CountsRead#countsForRelationship(int, int, int) */
    long countsForRelationship( KernelStatement statement, int startLabelId, int typeId, int endLabelId );

    /** @see org.neo4j.kernel.api.CountsRead#countsForRelationshipWithoutTxState(int, int, int) */
    long countsForRelationshipWithoutTxState( KernelStatement statement, int startLabelId, int typeId, int endLabelId );

    DoubleLongRegister indexUpdatesAndSize( KernelStatement statement, IndexDescriptor index,
            DoubleLongRegister target ) throws IndexNotFoundKernelException;

    DoubleLongRegister indexSample( KernelStatement statement, IndexDescriptor index, DoubleLongRegister target )
            throws IndexNotFoundKernelException;
}
