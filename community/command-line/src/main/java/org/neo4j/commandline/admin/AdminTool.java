/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.commandline.admin;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static java.lang.String.format;

public class AdminTool
{
    public static void main( String[] args )
    {
        Path homeDir = Paths.get( System.getenv( "NEO4J_HOME" ) );
        Path configDir = Paths.get( System.getenv( "NEO4J_CONF" ) );
        boolean debug = System.getenv( "NEO4J_DEBUG" ) != null;

        new AdminTool( CommandLocator.fromServiceLocator(), new RealOutsideWorld(), debug )
                .execute( homeDir, configDir, args );
    }

    private final String scriptName = "neo4j-admin";
    private final CommandLocator locator;
    private final OutsideWorld outsideWorld;
    private final boolean debug;
    private final Usage usage;

    public AdminTool( CommandLocator locator, OutsideWorld outsideWorld, boolean debug )
    {
        this.locator = CommandLocator.withAdditionalCommand( help(), locator );
        this.outsideWorld = outsideWorld;
        this.debug = debug;
        this.usage = new Usage( scriptName, this.locator );
    }

    public void execute( Path homeDir, Path configDir, String... args )
    {
        try
        {
            if ( args.length == 0 )
            {
                badUsage( "you must provide a command" );
                return;
            }
            String name = args[0];
            String[] commandArgs = Arrays.copyOfRange( args, 1, args.length );

            AdminCommand.Provider provider;
            try
            {
                provider = locator.findProvider( name );
            }
            catch ( NoSuchElementException e )
            {
                badUsage( format( "unrecognized command: %s", name ) );
                return;
            }

            AdminCommand command = provider.create( homeDir, configDir, outsideWorld );
            try
            {
                command.execute( commandArgs );
                success();
            }
            catch ( IncorrectUsage e )
            {
                badUsage( provider, e );
            }
            catch ( CommandFailed e )
            {
                commandFailed( e );
            }
        }
        catch ( RuntimeException e )
        {
            unexpected( e );
        }
    }

    private Supplier<AdminCommand.Provider> help()
    {
        return () -> new HelpCommand.Provider( usage );
    }

    private void badUsage( AdminCommand.Provider command, IncorrectUsage e )
    {
        final Usage.CommandUsage commandUsage = new Usage.CommandUsage( command, scriptName );
        commandUsage.print( outsideWorld::stdErrLine );
        failure( e.getMessage() );
    }

    private void badUsage( String message )
    {
        usage.print( outsideWorld::stdErrLine );
        failure( message );
    }

    private void unexpected( RuntimeException e )
    {
        failure( "unexpected error", e );
    }

    private void commandFailed( CommandFailed e )
    {
        failure( "command failed", e );
    }

    private void failure( String message, Exception e )
    {
        if ( debug )
        {
            failure( e, format( "%s: %s", message, e.getMessage() ) );
        }
        else
        {
            failure( e.getMessage() );
        }
    }

    private void failure( Exception e, String message )
    {
        outsideWorld.printStacktrace( e );
        failure( message );
    }

    private void failure( String message )
    {
        outsideWorld.stdErrLine( message );
        outsideWorld.exit( 1 );
    }

    private void success()
    {
        outsideWorld.exit( 0 );
    }
}
