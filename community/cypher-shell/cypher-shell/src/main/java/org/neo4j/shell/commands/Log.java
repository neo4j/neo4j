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

import org.neo4j.shell.CypherShell;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.Logger;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.neo4j.shell.log.Logger.setupLogging;

/**
 * Enable logging.
 */
public record Log( CypherShell shell ) implements Command
{
    @Override
    public void execute( final List<String> args ) throws ExitException, CommandException
    {
        requireArgumentCount( args, 0, 1 );

        var level = args.size() == 0 ? Logger.Level.defaultActiveLevel() : parse( args.get( 0 ) );
        setLevel( level );
    }

    private void setLevel( Logger.Level level ) throws CommandException
    {
        setupLogging( level.javaLevel() );
        if ( shell.isConnected() )
        {
            // We need to re-connect in order to propagate the new logging level to driver.
            shell.reconnect();
        }
    }

    private Logger.Level parse( String input ) throws CommandException
    {
        try
        {
            return Logger.Level.from( input );
        }
        catch ( Exception e )
        {
            throw new CommandException( "Failed to parse " + input + "\n" + metadata().usage() );
        }
    }

    public static class Factory implements Command.Factory
    {
        @Override
        public Metadata metadata()
        {
            var help = "Enables or disables logging to the standard error output stream.";
            var levels = stream( Logger.Level.values() ).map( l -> l.name().toLowerCase() ).collect( joining( ", " ) );
            var usage = "<level>\n\nwhere <level> is one of " + levels + ". Defaults to debug if level is not specified.";

            return new Metadata( ":log", "Enable logging", usage, help, List.of() );
        }

        @Override
        public Command executor( Arguments args )
        {
            return new Log( args.cypherShell() );
        }
    }
}
