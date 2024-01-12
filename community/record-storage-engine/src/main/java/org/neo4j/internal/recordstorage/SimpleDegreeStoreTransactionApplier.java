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
package org.neo4j.internal.recordstorage;

import java.util.function.Supplier;
import org.neo4j.internal.counts.DegreeUpdater;

/**
 * It's simple because it doesn't open updates if they are not required and avoids other hacks of {@link CountsStoreTransactionApplier}
 */
class SimpleDegreeStoreTransactionApplier extends TransactionApplier.Adapter {
    private final Supplier<DegreeUpdater> degreeUpdaterSupplier;
    private DegreeUpdater degreesUpdater;

    SimpleDegreeStoreTransactionApplier(Supplier<DegreeUpdater> degreeUpdaterSupplier) {
        this.degreeUpdaterSupplier = degreeUpdaterSupplier;
    }

    @Override
    public boolean visitGroupDegreeCommand(Command.GroupDegreeCommand command) {
        degreesUpdater().increment(command.groupId(), command.direction(), command.delta());
        return false;
    }

    @Override
    public void close() {
        if (degreesUpdater != null) {
            degreesUpdater.close();
        }
    }

    private DegreeUpdater degreesUpdater() {
        if (degreesUpdater == null) {
            degreesUpdater = degreeUpdaterSupplier.get();
        }
        return degreesUpdater;
    }
}
