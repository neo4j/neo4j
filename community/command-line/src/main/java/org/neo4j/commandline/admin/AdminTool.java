/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.commandline.admin;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;

import static java.lang.String.format;

public class AdminTool
{
    public static void main( String[] args )
    {
        Path homeDir = Paths.get( System.getenv( "NEO4J_HOME" ) );
        Path configDir = Paths.get( System.getenv( "NEO4J_CONF" ) );
        String extraHelp = System.getenv( "NEO4J_EXTRA_HELP" );
        boolean debug = System.getenv( "NEO4J_DEBUG" ) != null;

        new AdminTool( new ServiceCommandLocator(), System.out::println, extraHelp, debug )
                .execute( homeDir, configDir, args )
                .exit();
    }

    private final String scriptName = "neo4j-admin";
    private final CommandLocator locator;
    private final Output out;
    private final boolean debug;
    private final Usage usage;

    public AdminTool( CommandLocator locator, Output out, String extraHelp, boolean debug )
    {
        this.locator = new AppendingLocator( help(), locator );
        this.out = out;
        this.debug = debug;
        this.usage = new Usage( scriptName, out, this.locator, extraHelp );
    }

    public Result execute( Path homeDir, Path configDir, String... args )
    {
        try
        {
            String name = args[0];
            String[] commandArgs = Arrays.copyOfRange( args, 1, args.length );

            AdminCommand.Provider provider;
            try
            {
                provider = locator.apply( name );
            }
            catch ( NoSuchElementException e )
            {
                return badUsage( name, commandArgs );
            }

            AdminCommand command = provider.create( homeDir, configDir );
            try
            {
                command.execute( commandArgs );
                return success();
            }
            catch ( IncorrectUsage e )
            {
                return badUsage( provider, e );
            }
            catch ( CommandFailed e )
            {
                return failure( e );
            }
        }
        catch ( RuntimeException e )
        {
            return unexpected( e );
        }
    }

    private Supplier<AdminCommand.Provider> help()
    {
        return () -> new HelpCommand.Provider( usage );
    }

    private Result badUsage( AdminCommand.Provider command, IncorrectUsage e )
    {
        new Usage.CommandUsage( command, out, scriptName ).print();
        return failure( e.getMessage() );
    }

    private Result badUsage( String name, String[] commandArgs )
    {
        usage.print();
        String message = commandArgs.length == 0 ? format( "unrecognized command: %s", name ) : commandArgs[0];
        return failure( message );
    }

    private Result unexpected( RuntimeException e )
    {
        return failure( e, "unexpected error: " + e.getMessage() );
    }

    private Result failure( Throwable e )
    {
        if ( debug )
        {
            return failure( e, e.getMessage() );
        }
        else
        {
            return failure( e.getMessage() );
        }
    }

    private Result failure( Throwable e, String message )
    {
        return () -> {
            e.printStackTrace();
            failure( message ).exit();
        };
    }

    private static Result failure( String message )
    {
        return () -> {
            System.err.println( message );
            System.exit( 1 );
        };
    }

    private static Result success()
    {
        return () -> System.exit( 0 );
    }

    private interface Result
    {
        void exit();
    }

    public interface CommandLocator
            extends Function<String, AdminCommand.Provider>, Supplier<Iterable<AdminCommand.Provider>>
    {
    }

    private static class ServiceCommandLocator implements CommandLocator
    {
        @Override
        public AdminCommand.Provider apply( String name )
        {
            return Service.load( AdminCommand.Provider.class, name );
        }

        @Override
        public Iterable<AdminCommand.Provider> get()
        {
            return Service.load( AdminCommand.Provider.class );
        }
    }

    private class AppendingLocator implements CommandLocator
    {
        private final Supplier<AdminCommand.Provider> command;
        private final CommandLocator commands;

        public AppendingLocator( Supplier<AdminCommand.Provider> command, CommandLocator commands )
        {
            this.command = command;
            this.commands = commands;
        }

        @Override
        public AdminCommand.Provider apply( String name )
        {
            AdminCommand.Provider provider = command.get();
            return Objects.equals( name, provider.name() ) ? provider : commands.apply( name );
        }

        @Override
        public Iterable<AdminCommand.Provider> get()
        {
            return Iterables.append( command.get(), commands.get() );
        }
    }
}
