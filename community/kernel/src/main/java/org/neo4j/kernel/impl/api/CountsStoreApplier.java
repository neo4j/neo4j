/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.io.IOException;

import org.neo4j.function.Function;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;

public class CountsStoreApplier extends CommandHandler.Adapter
{
    static final Function<CountsAccessor.Updater,CommandHandler> FACTORY =
            new Function<CountsAccessor.Updater,CommandHandler>()
            {
                @Override
                public CommandHandler apply( CountsAccessor.Updater updater )
                {
                    return new CountsStoreApplier( updater );
                }
            };
    private final CountsAccessor.Updater countsUpdater;

    public CountsStoreApplier( CountsAccessor.Updater countsUpdater )
    {
        this.countsUpdater = countsUpdater;
    }

    @Override
    public void close()
    {
        countsUpdater.close();
    }

    @Override
    public boolean visitNodeCountsCommand( Command.NodeCountsCommand command )
    {
        countsUpdater.incrementNodeCount( command.labelId(), command.delta() );
        return false;
    }

    @Override
    public boolean visitRelationshipCountsCommand( Command.RelationshipCountsCommand command ) throws IOException
    {
        countsUpdater.incrementRelationshipCount(
                command.startLabelId(), command.typeId(), command.endLabelId(), command.delta() );
        return false;
    }
}
