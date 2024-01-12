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
import org.neo4j.internal.recordstorage.Command.SchemaRuleCommand;

class MultiVersionCountsStoreTransactionApplier extends TransactionApplier.Adapter {
    private final Supplier<CountsUpdater> updaterSupplier;
    private CountsUpdater countsUpdater;
    private boolean haveUpdates;
    private boolean countsUpdaterClosed;

    MultiVersionCountsStoreTransactionApplier(Supplier<CountsUpdater> updaterSupplier) {
        this.updaterSupplier = updaterSupplier;
    }

    @Override
    public void close() {
        closeCountsUpdatersIfOpen();
    }

    private void closeCountsUpdatersIfOpen() {
        // It's important to call the countsUpdater() method which opens the updater if it wasn't already open, this is
        // because
        // we need to register all transactions to the counts store even if they don't apply any counts changes.
        if (!countsUpdaterClosed) {
            countsUpdater().close();
            countsUpdaterClosed = true;
        }
    }

    @Override
    public boolean visitNodeCountsCommand(Command.NodeCountsCommand command) {
        haveUpdates = true;
        countsUpdater().incrementNodeCount(command.labelId(), command.delta());
        return false;
    }

    /**
     * @return Updater for counts. This is retrieved lazily on first counts change because the window of time a counts updater is open
     * affects the window of time that all counts-updating transactions will need to block during a checkpoint. So instead of opening this updater
     * when the transaction starts to apply then open it when it gets to changing counts, if at all.
     */
    private CountsUpdater countsUpdater() {
        if (countsUpdater == null) {
            countsUpdater = updaterSupplier.get();
        }
        return countsUpdater;
    }

    @Override
    public boolean visitRelationshipCountsCommand(Command.RelationshipCountsCommand command) {
        haveUpdates = true;
        countsUpdater()
                .incrementRelationshipCount(
                        command.startLabelId(), command.typeId(), command.endLabelId(), command.delta());
        return false;
    }

    @Override
    public boolean visitSchemaRuleCommand(SchemaRuleCommand command) {
        // This shows that this transaction is a schema transaction, so it cannot have commands
        // updating any counts anyway. Therefore the counts updater is closed right away.
        // This also breaks an otherwise deadlocking scenario between check pointer, this applier
        // and an index population thread wanting to apply index sampling to the counts store.
        assert !haveUpdates
                : "Assumed that a schema transaction wouldn't also contain data commands affecting "
                        + "counts store, but was proven wrong with this transaction";
        closeCountsUpdatersIfOpen();
        return false;
    }
}
