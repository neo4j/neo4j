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
package org.neo4j.kernel.impl.api;

import java.time.Clock;
import org.neo4j.internal.id.IdController;
import org.neo4j.storageengine.api.TransactionIdStore;

/**
 * Used to inject additional conditions (e.g. delays) to ID reuse, i.e. when a deleted ID can be available for reuse again.
 */
@FunctionalInterface
public interface ExternalIdReuseConditionProvider {
    ExternalIdReuseConditionProvider NONE = (transactionIdStore, clock) -> snapshot -> true;

    IdController.IdFreeCondition get(TransactionIdStore transactionIdStore, Clock clock);
}
