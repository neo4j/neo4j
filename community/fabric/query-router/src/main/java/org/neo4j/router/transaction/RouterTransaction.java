/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.router.transaction;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.fabric.executor.Location;
import org.neo4j.fabric.transaction.TransactionMode;
import org.neo4j.fabric.transaction.parent.CompoundTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.query.ConstituentTransactionFactory;
import org.neo4j.router.impl.query.StatementType;

public interface RouterTransaction extends CompoundTransaction<DatabaseTransaction> {
    Optional<Status> getReasonIfTerminated();

    DatabaseTransaction transactionFor(Location location, TransactionMode mode);

    void verifyStatementType(StatementType type);

    void setMetaData(Map<String, Object> txMeta);

    void throwIfTerminatedOrClosed(Supplier<String> closedExceptionMessage);

    void setConstituentTransactionFactory(ConstituentTransactionFactory constituentTransactionFactory);
}
