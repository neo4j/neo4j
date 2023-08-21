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
import org.neo4j.counts.CountsUpdater;
import org.neo4j.internal.counts.DegreeUpdater;

/**
 * It's simple because it doesn't open updates if they are not required and avoids other hacks of {@link CountsStoreTransactionApplier}
 */
class SimpleCountsStoreTransactionApplier extends TransactionApplier.Adapter {
    private final Supplier<CountsUpdater> counstUpdaterSupplier;
    private final Supplier<DegreeUpdater> degreeUpdaterSupplier;
    private CountsUpdater countsUpdater;
    private DegreeUpdater degreesUpdater;

    SimpleCountsStoreTransactionApplier(
            Supplier<CountsUpdater> countsUpdaterSupplier, Supplier<DegreeUpdater> degreeUpdaterSupplier) {
        this.counstUpdaterSupplier = countsUpdaterSupplier;
        this.degreeUpdaterSupplier = degreeUpdaterSupplier;
    }

    @Override
    public boolean visitNodeCountsCommand(Command.NodeCountsCommand command) {
        countsUpdater().incrementNodeCount(command.labelId(), command.delta());
        return false;
    }

    @Override
    public boolean visitRelationshipCountsCommand(Command.RelationshipCountsCommand command) {
        countsUpdater()
                .incrementRelationshipCount(
                        command.startLabelId(), command.typeId(), command.endLabelId(), command.delta());
        return false;
    }

    @Override
    public boolean visitGroupDegreeCommand(Command.GroupDegreeCommand command) {
        degreesUpdater().increment(command.groupId(), command.direction(), command.delta());
        return false;
    }

    @Override
    public void close() {
        if (countsUpdater != null) {
            countsUpdater.close();
        }
        if (degreesUpdater != null) {
            degreesUpdater.close();
        }
    }

    private CountsUpdater countsUpdater() {
        if (countsUpdater == null) {
            countsUpdater = counstUpdaterSupplier.get();
        }
        return countsUpdater;
    }

    private DegreeUpdater degreesUpdater() {
        if (degreesUpdater == null) {
            degreesUpdater = degreeUpdaterSupplier.get();
        }
        return degreesUpdater;
    }
}
