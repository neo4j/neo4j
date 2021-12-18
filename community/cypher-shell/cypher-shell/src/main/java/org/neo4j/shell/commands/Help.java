/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.shell.commands.CommandHelper.CommandFactoryHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.log.AnsiFormattedText;
import org.neo4j.shell.log.Logger;

import static java.util.Comparator.comparing;

/**
 * Help command, which prints help documentation.
 */
public class Help implements Command
{
    public static final String CYPHER_MANUAL_LINK = "https://neo4j.com/docs/cypher-manual/current/";
    private final Logger logger;
    private final CommandFactoryHelper commandHelper;

    public Help( final Logger shell, final CommandFactoryHelper commandHelper )
    {
        this.logger = shell;
        this.commandHelper = commandHelper;
    }

    @Override
    public void execute( final List<String> args ) throws CommandException
    {
        requireArgumentCount( args, 0, 1 );
        if ( args.size() == 0 )
        {
            printGeneralHelp();
        }
        else
        {
            printHelpFor( args.get( 0 ) );
        }
    }

    private void printHelpFor( final String name ) throws CommandException
    {
        var cmd = commandHelper.factoryByName( name );
        if ( cmd == null && !name.startsWith( ":" ) )
        {
            // Be friendly to users and don't force them to type colons for help if possible
            cmd = commandHelper.factoryByName( ":" + name );
        }

        if ( cmd == null )
        {
            throw new CommandException( AnsiFormattedText.from( "No such command: " ).bold( name ) );
        }

        logger.printOut( AnsiFormattedText.from( "\nusage: " )
                                          .bold( cmd.metadata().name() )
                                          .append( " " )
                                          .append( cmd.metadata().usage() )
                                          .append( "\n\n" )
                                          .append( cmd.metadata().help() )
                                          .append( "\n" )
                                          .formattedString() );
    }

    private void printGeneralHelp()
    {
        logger.printOut( "\nAvailable commands:" );

        var allCommands = commandHelper.factories().stream()
                .map( Command.Factory::metadata )
                .sorted( comparing( Metadata::name ) )
                .toList();

        int leftColWidth = longestCmdLength( allCommands );

        allCommands.forEach( cmd -> logger.printOut(
                AnsiFormattedText.from( "  " )
                                 .bold( String.format( "%-" + leftColWidth + "s", cmd.name() ) )
                                 .append( " " + cmd.description() )
                                 .formattedString() ) );

        logger.printOut( "\nFor help on a specific command type:" );
        logger.printOut( AnsiFormattedText.from( "    " )
                                          .append( metadata().name() )
                                          .bold( " command" )
                                          .append( "\n" ).formattedString() );

        logger.printOut( "Keyboard shortcuts:" );
        logger.printOut( "    Up and down arrows to access statement history." );
        logger.printOut( "    Tab for autocompletion of commands." );

        logger.printOut( "\nFor help on cypher please visit:" );
        logger.printOut( AnsiFormattedText.from( "    " )
                                          .append( CYPHER_MANUAL_LINK )
                                          .append( "\n" ).formattedString() );
    }

    private static int longestCmdLength( List<Command.Metadata> allCommands )
    {
        return allCommands.stream().mapToInt( m -> m.name().length() ).max().orElse( 0 );
    }

    public static class Factory implements Command.Factory
    {
        @Override
        public Metadata metadata()
        {
            var help = "Show the list of available commands or help for a specific command.";
            return new Metadata( ":help", "Show this help message", "[command]", help, List.of( ":man" ) );
        }

        @Override
        public Command executor( Arguments args )
        {
            return new Help( args.logger(), new CommandFactoryHelper() );
        }
    }
}
