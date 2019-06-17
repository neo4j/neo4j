/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.Collection;

import org.neo4j.counts.CountsVisitor;
import org.neo4j.storageengine.api.CountsDelta;
import org.neo4j.storageengine.api.StorageCommand;

/**
 * A {@link CountsDelta} with an additional capability of turning counts into {@link StorageCommand commands} for storage.
 */
public class CountsRecordState extends CountsDelta implements RecordState
{
    @Override
    public void extractCommands( Collection<StorageCommand> target )
    {
        accept( new CommandCollector( target ) );
    }

    // CountsDelta already implements hasChanges

    private static class CommandCollector extends CountsVisitor.Adapter
    {
        private final Collection<StorageCommand> commands;

        CommandCollector( Collection<StorageCommand> commands )
        {
            this.commands = commands;
        }

        @Override
        public void visitNodeCount( int labelId, long count )
        {
            if ( count != 0 )
            {   // Only add commands for counts that actually change
                commands.add( new Command.NodeCountsCommand( labelId, count ) );
            }
        }

        @Override
        public void visitRelationshipCount( int startLabelId, int typeId, int endLabelId, long count )
        {
            if ( count != 0 )
            {   // Only add commands for counts that actually change
                commands.add( new Command.RelationshipCountsCommand( startLabelId, typeId, endLabelId, count ) );
            }
        }
    }
}
