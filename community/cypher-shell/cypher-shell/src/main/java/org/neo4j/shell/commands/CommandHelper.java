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
package org.neo4j.shell.commands;

import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.shell.CypherShell;
import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.DuplicateCommandException;
import org.neo4j.shell.log.AnsiFormattedText;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.ShellStatementParser;

/**
 * Utility methods for dealing with commands
 */
public class CommandHelper
{
    private final TreeMap<String, Command> commands = new TreeMap<>( String.CASE_INSENSITIVE_ORDER );

    public CommandHelper( Logger logger, Historian historian, CypherShell cypherShell )
    {
        registerAllCommands( logger, historian, cypherShell );
    }

    /**
     * Split an argument string on whitespace
     */
    @Nonnull
    public static String[] simpleArgParse( @Nonnull final String argString, int expectedCount,
                                           @Nonnull final String commandName, @Nonnull final String usage )
            throws CommandException
    {
        return simpleArgParse( argString, expectedCount, expectedCount, commandName, usage );
    }

    /**
     * Split an argument string on whitespace
     */
    @Nonnull
    public static String[] simpleArgParse( @Nonnull final String argString, int minCount, int maxCount,
                                           @Nonnull final String commandName, @Nonnull final String usage )
            throws CommandException
    {
        final String[] args;
        if ( argString.trim().isEmpty() )
        {
            args = new String[] {};
        }
        else
        {
            args = argString.trim().split( "\\s+" );
        }

        if ( args.length < minCount || args.length > maxCount )
        {
            throw new CommandException( AnsiFormattedText.from( "Incorrect number of arguments.\nusage: " )
                                                         .bold().append( commandName ).boldOff().append( " " ).append( usage ) );
        }

        return args;
    }

    private void registerAllCommands( Logger logger,
                                      Historian historian,
                                      CypherShell cypherShell )
    {
        registerCommand( new Exit( logger ) );
        registerCommand( new Help( logger, this ) );
        registerCommand( new History( logger, historian ) );
        registerCommand( new Use( cypherShell ) );
        registerCommand( new Begin( cypherShell ) );
        registerCommand( new Commit( cypherShell ) );
        registerCommand( new Rollback( cypherShell ) );
        registerCommand( new Param( cypherShell.getParameterMap() ) );
        registerCommand( new Params( logger, cypherShell.getParameterMap() ) );
        registerCommand( new Source( cypherShell, new ShellStatementParser() ) );
    }

    private void registerCommand( @Nonnull final Command command ) throws DuplicateCommandException
    {
        if ( commands.containsKey( command.getName() ) )
        {
            throw new DuplicateCommandException( "This command name has already been registered: " + command.getName() );
        }

        commands.put( command.getName(), command );

        for ( String alias : command.getAliases() )
        {
            if ( commands.containsKey( alias ) )
            {
                throw new DuplicateCommandException( "This command alias has already been registered: " + alias );
            }
            commands.put( alias, command );
        }
    }

    /**
     * Get a command corresponding to the given name, or null if no such command has been registered.
     */
    @Nullable
    public Command getCommand( @Nonnull final String name )
    {
        if ( commands.containsKey( name ) )
        {
            return commands.get( name );
        }
        return null;
    }

    /**
     * Get a list of all registered commands
     */
    @Nonnull
    public List<Command> getAllCommands()
    {
        return commands.values().stream().distinct().collect( Collectors.toList() );
    }
}
