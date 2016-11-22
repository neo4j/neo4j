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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import static java.lang.String.format;

public class AdminTool
{
    public static void main( String[] args ) throws IOException
    {
        Path homeDir = Paths.get( System.getenv().getOrDefault( "NEO4J_HOME", "" ) );
        Path configDir = Paths.get( System.getenv().getOrDefault( "NEO4J_CONF", "" ) );
        boolean debug = System.getenv( "NEO4J_DEBUG" ) != null;

        try ( RealOutsideWorld outsideWorld = new RealOutsideWorld() )
        {
            new AdminTool( CommandLocator.fromServiceLocator(), BlockerLocator.fromServiceLocator(), outsideWorld,
                    debug ).execute( homeDir, configDir, args );
        }
    }

    private final String scriptName = "neo4j-admin";
    private final CommandLocator commandLocator;
    private final BlockerLocator blockerLocator;
    private final OutsideWorld outsideWorld;
    private final boolean debug;
    private final Usage usage;

    public AdminTool( CommandLocator commandLocator, BlockerLocator blockerLocator, OutsideWorld outsideWorld,
            boolean debug )
    {
        this.commandLocator = CommandLocator.withAdditionalCommand( help(), commandLocator );
        this.blockerLocator = blockerLocator;
        this.outsideWorld = outsideWorld;
        this.debug = debug;
        this.usage = new Usage( scriptName, this.commandLocator );
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
                provider = commandLocator.findProvider( name );
                for ( AdminCommand.Blocker blocker : blockerLocator.findBlockers( name ) )
                {
                    if ( blocker.doesBlock( homeDir, configDir ) )
                    {
                        commandFailed( new CommandFailed( blocker.explanation() ) );
                    }
                }
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
        outsideWorld.stdErrLine( e.getMessage() );
        outsideWorld.stdErrLine( "" );
        usage.printUsageForCommand( command, outsideWorld::stdErrLine );
        failure();
    }

    private void badUsage( String message )
    {
        outsideWorld.stdErrLine( message );
        usage.print( outsideWorld::stdErrLine );
        failure();
    }

    private void unexpected( RuntimeException e )
    {
        failure( "unexpected error", e );
    }

    private void commandFailed( CommandFailed e )
    {
        failure( "command failed", e );
    }

    private void failure()
    {
        outsideWorld.exit( 1 );
    }

    private void failure( String message, Exception e )
    {
        if ( debug )
        {
            outsideWorld.printStacktrace( e );
        }
        failure( format( "%s: %s", message, e.getMessage() ) );
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
