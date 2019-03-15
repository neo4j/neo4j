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

import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;

import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.internal.recordstorage.Command.PropertyCommand;

/**
 * Groups property commands by entity. The commands are provided from a list of transaction commands.
 * Most entity updates include both the entity command as well as property commands, but sometimes
 * only property commands for an entity exists in the list and this grouper handles both scenarios.
 * Commands are appended to an array and then sorted before handed over for being processed.
 */
public class EntityCommandGrouper<ENTITY extends Command>
{
    private final Comparator<Command> COMMAND_COMPARATOR = new Comparator<Command>()
    {
        @Override
        public int compare( Command o1, Command o2 )
        {
            int entityIdComparison = Long.compare( entityId( o1 ), entityId( o2 ) );
            return entityIdComparison != 0 ? entityIdComparison : Integer.compare( commandType( o1 ), commandType( o2 ) );
        }

        private long entityId( Command command )
        {
            if ( command.getClass() == entityCommandClass )
            {
                return command.getKey();
            }
            return ((PropertyCommand) command).getEntityId();
        }

        private int commandType( Command command )
        {
            if ( command.getClass() == entityCommandClass )
            {
                return 0;
            }
            return 1;
        }
    };

    private final Class<ENTITY> entityCommandClass;
    private Command[] commands;
    private int writeCursor;
    private int readCursor;
    private long currentEntity;
    private ENTITY currentEntityCommand;

    public EntityCommandGrouper( Class<ENTITY> entityCommandClass, int sizeHint )
    {
        this.entityCommandClass = entityCommandClass;
        this.commands = new Command[sizeHint];
    }

    public void add( Command command )
    {
        if ( writeCursor == commands.length )
        {
            commands = Arrays.copyOf( commands, commands.length * 2 );
        }
        commands[writeCursor++] = command;
    }

    public void sort()
    {
        Arrays.sort( commands, 0, writeCursor, COMMAND_COMPARATOR );
    }

    public boolean nextEntity()
    {
        if ( readCursor >= writeCursor )
        {
            return false;
        }

        if ( commands[readCursor].getClass() == entityCommandClass )
        {
            currentEntityCommand = (ENTITY) commands[readCursor++];
            currentEntity = currentEntityCommand.getKey();
        }
        else
        {
            PropertyCommand firstPropertyCommand = (PropertyCommand) commands[readCursor];
            currentEntityCommand = null;
            currentEntity = firstPropertyCommand.getEntityId();
        }
        return true;
    }

    public long getCurrentEntity()
    {
        return currentEntity;
    }

    public ENTITY getCurrentEntityCommand()
    {
        return currentEntityCommand;
    }

    public PropertyCommand nextProperty()
    {
        if ( readCursor < writeCursor )
        {
            Command command = commands[readCursor];
            if ( command instanceof PropertyCommand && ((PropertyCommand) command).getEntityId() == currentEntity )
            {
                readCursor++;
                return (PropertyCommand) command;
            }
        }
        return null;
    }

    public void clear()
    {
        if ( writeCursor > 1_000 )
        {
            // Don't continue to hog large transactions
            Arrays.fill( commands, 1_000, writeCursor, null );
        }
        writeCursor = 0;
        readCursor = 0;
    }

    public LongIterable entityIds()
    {
        LongArrayList list = new LongArrayList();
        int cursor = 0;
        long currentNode = -1;
        while ( cursor < writeCursor )
        {
            Command candidate = commands[cursor++];
            long nodeId = candidate.getClass() == entityCommandClass ? candidate.getKey() : ((PropertyCommand) candidate).getEntityId();
            if ( nodeId != currentNode )
            {
                currentNode = nodeId;
                list.add( currentNode );
            }
        }
        return list;
    }
}
