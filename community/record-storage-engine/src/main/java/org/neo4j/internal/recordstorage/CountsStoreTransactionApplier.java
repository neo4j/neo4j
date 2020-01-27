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
package org.neo4j.internal.recordstorage;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.recordstorage.Command.SchemaRuleCommand;

class CountsStoreTransactionApplier extends TransactionApplier.Adapter
{
    private final CountsAccessor.Updater countsUpdater;
    private boolean haveUpdates;
    private boolean countsUpdaterClosed;

    CountsStoreTransactionApplier( CountsAccessor.Updater countsUpdater )
    {
        this.countsUpdater = countsUpdater;
    }

    @Override
    public void close()
    {
        closeCountsUpdaterIfOpen();
    }

    private void closeCountsUpdaterIfOpen()
    {
        if ( !countsUpdaterClosed )
        {
            countsUpdater.close();
            countsUpdaterClosed = true;
        }
    }

    @Override
    public boolean visitNodeCountsCommand( Command.NodeCountsCommand command )
    {
        haveUpdates = true;
        countsUpdater.incrementNodeCount( command.labelId(), command.delta() );
        return false;
    }

    @Override
    public boolean visitRelationshipCountsCommand( Command.RelationshipCountsCommand command )
    {
        haveUpdates = true;
        countsUpdater.incrementRelationshipCount( command.startLabelId(), command.typeId(), command.endLabelId(), command.delta() );
        return false;
    }

    @Override
    public boolean visitSchemaRuleCommand( SchemaRuleCommand command )
    {
        // This shows that this transaction is a schema transaction, so it cannot have commands
        // updating any counts anyway. Therefore the counts updater is closed right away.
        // This also breaks an otherwise deadlocking scenario between check pointer, this applier
        // and an index population thread wanting to apply index sampling to the counts store.
        assert !haveUpdates : "Assumed that a schema transaction wouldn't also contain data commands affecting " +
                "counts store, but was proven wrong with this transaction";
        closeCountsUpdaterIfOpen();
        return false;
    }
}
