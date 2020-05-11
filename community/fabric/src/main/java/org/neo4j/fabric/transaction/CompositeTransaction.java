/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.fabric.transaction;

import java.util.function.Supplier;

import org.neo4j.fabric.executor.FabricException;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.executor.SingleDbTransaction;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * A container for {@link SingleDbTransaction}s.
 */
public interface CompositeTransaction
{
    /**
     * Starts and registers a transaction that is known to do writes.
     */
    <TX extends SingleDbTransaction> TX startWritingTransaction( Location location, Supplier<TX> writingTransactionSupplier ) throws FabricException;

    /**
     * Starts and registers a transaction that is so far known to do only reads. Such transaction can be later upgraded to a writing
     * one using {@link #upgradeToWritingTransaction(SingleDbTransaction)}
     */
    <TX extends SingleDbTransaction> TX startReadingTransaction( Location location, Supplier<TX> readingTransactionSupplier ) throws FabricException;

    /**
     * Starts and registers a transaction that will do only reads. Such transaction cannot be later upgraded to a writing
     * one using {@link #upgradeToWritingTransaction(SingleDbTransaction)}
     */
    <TX extends SingleDbTransaction> TX startReadingOnlyTransaction( Location location, Supplier<TX> readingTransactionSupplier ) throws FabricException;

    <TX extends SingleDbTransaction> void upgradeToWritingTransaction( TX writingTransaction ) throws FabricException;

    void childTransactionTerminated( Status reason );
}
