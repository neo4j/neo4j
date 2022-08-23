/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.exceptions;

import static org.neo4j.kernel.api.exceptions.Status.Transaction.LeaseExpired;

import java.util.List;
import java.util.function.Predicate;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.impl.api.LeaseException;

public class TransientFailurePredicate implements Predicate<Throwable> {
    private static final List<Class<? extends Throwable>> transientFailureClasses = List.of(
            LeaseException.class, TransientTransactionFailureException.class, WriteOperationsNotAllowedException.class);

    @Override
    public boolean test(Throwable error) {
        if (isLockExpired(error)) {
            return true;
        }
        return transientFailureClasses.stream().anyMatch(clazz -> clazz.isInstance(error));
    }

    private static boolean isLockExpired(Throwable error) {
        return error instanceof TransactionFailureException
                        && ((TransactionFailureException) error).status() == LeaseExpired
                || error.getCause() instanceof TransactionFailureException
                        && ((TransactionFailureException) error.getCause()).status() == LeaseExpired;
    }
}
