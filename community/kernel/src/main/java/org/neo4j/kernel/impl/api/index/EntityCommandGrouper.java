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
package org.neo4j.kernel.impl.api.index;

import java.util.Arrays;
import java.util.Comparator;

import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;

/**
 * Groups property commands by entity. The commands are provided from a list of transaction commands.
 * Most entity updates include both the entity command as well as property commands, but sometimes
 * only property commands for an entity exists in the list and this grouper handles both scenarios.
 * Commands are appended to an array and then sorted before handed over for being processed.
 * Hence one entity group can look like any of these combinations:
 * <ul>
 *     <li>Entity command ({@link NodeCommand} or {@link RelationshipCommand} followed by zero or more {@link PropertyCommand property commands}
 *     for that entity</li>
 *     <li>zero or more {@link PropertyCommand property commands}, all for the same node</li>
 * </ul>
 * <p>
 * Typical interaction goes like this:
 * <ol>
 *     <li>All commands are added with {@link #add(Command)}</li>
 *     <li>Get a cursor to the sorted commands using {@link #sortAndAccessGroups()}</li>
 *     <li>Call {@link #clear()} and use this instance again for another set of commands</li>
 * </ol>
 */
public class EntityCommandGrouper<ENTITY extends Command>
{
    /**
     * Enforces the order described on the class-level javadoc above.
     */
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
            return command.getClass() == entityCommandClass ? 0 : 1;
        }
    };

    private final Class<ENTITY> entityCommandClass;
    private Command[] commands;
    private int writeCursor;

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

    public Cursor sortAndAccessGroups()
    {
        Arrays.sort( commands, 0, writeCursor, COMMAND_COMPARATOR );
        return new Cursor();
    }

    public void clear()
    {
        if ( writeCursor > 1_000 )
        {
            // Don't continue to hog large transactions
            Arrays.fill( commands, 1_000, writeCursor, null );
        }
        writeCursor = 0;
    }

    /**
     * Interaction goes like this:
     * <ol>
     *     <li>Call {@link #nextEntity()} to go to the next group, if any</li>
     *     <li>A group may or may not have the entity command, as accessed by {@link #currentEntityCommand()},
     *         either way the entity id is accessible using {@link #currentEntityId()}</li>
     *     <li>Call {@link #nextProperty()} until it returns null, now all the {@link PropertyCommand} in this group have been accessed</li>
     * </ol>
     */
    public class Cursor
    {
        private int readCursor;
        private long currentEntity;
        private ENTITY currentEntityCommand;

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

        public long currentEntityId()
        {
            return currentEntity;
        }

        public ENTITY currentEntityCommand()
        {
            return currentEntityCommand;
        }
    }
}
