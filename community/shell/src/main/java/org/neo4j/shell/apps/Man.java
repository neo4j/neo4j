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
package org.neo4j.shell.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.AppShellServer;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.impl.AbstractAppServer;

import static org.neo4j.helpers.Args.splitLongLine;

/**
 * Prints a short manual for an {@link App}.
 */
@Service.Implementation( App.class )
public class Man extends AbstractApp
{
    public static final int CONSOLE_WIDTH = 80;

    private static Collection<String> availableCommands;

    public Man()
    {
        addOptionDefinition( "l", new OptionDefinition( OptionValueType.NONE,
                "Display the commands in a vertical list" ) );
    }

    @Override
    public Continuation execute( AppCommandParser parser, Session session,
        Output out ) throws Exception
    {
        if ( parser.arguments().size() == 0 )
        {
            boolean list = parser.options().containsKey( "l" );
            printHelpString( out, getServer(), list );
            return Continuation.INPUT_COMPLETE;
        }

        App app = this.getApp( parser );
        out.println( "" );
        for ( String line : splitLongLine( fixDesciption( app.getDescription() ),
                CONSOLE_WIDTH ) )
        {
            out.println( line );
        }
        println( out, "" );
        boolean hasOptions = false;
        for ( String option : app.getAvailableOptions() )
        {
            hasOptions = true;
            String description = fixDesciption( app.getDescription( option ) );
            String[] descriptionLines = splitLongLine( description, CONSOLE_WIDTH );
            for ( int i = 0; i < descriptionLines.length; i++ )
            {
                String line = "";
                if ( i == 0 )
                {
                    String optionPrefix = option.length() > 1 ? "--" : "-";
                    line = optionPrefix + option;
                }
                line += "\t ";
                line += descriptionLines[ i ];
                println( out, line );
            }
        }
        if ( hasOptions )
        {
            println( out, "" );
        }
        return Continuation.INPUT_COMPLETE;
    }

    private static String getShortUsageString()
    {
        return "man <command>";
    }

    private String fixDesciption( String description )
    {
        if ( description == null )
        {
            description = "";
        }
        else if ( !description.endsWith( "." ) )
        {
            description = description + ".";
        }
        return description;
    }

    private void println( Output out, String string ) throws RemoteException
    {
        out.println( "  " + string );
    }

    private App getApp( AppCommandParser parser ) throws Exception
    {
        String appName = parser.arguments().get( 0 ).toLowerCase();
        App app = this.getServer().findApp( appName );
        if ( app == null )
        {
            throw new ShellException( "No manual entry for '" + appName +
                "'" );
        }
        return app;
    }

    @Override
    public String getDescription()
    {
        return "Display a manual for a command or a general help message.\n" +
            "Usage: " + getShortUsageString();
    }

    /**
     * Utility method for getting a short help string for a server. Basically it
     * contains an introductory message and also lists all available apps for
     * the server.
     *
     * @param out the output
     * @param server the server to ask for
     * @param list if {@code true}, a list of the commands is printed
     * @throws RemoteException in case of remoting errors
     */
    public static void printHelpString( Output out, ShellServer server, boolean list )
            throws RemoteException
    {
        String header = "Available commands:";
        if ( list )
        {
            out.println( header );
            out.println();
            for ( String command : server.getAllAvailableCommands() )
            {
                out.println( "   " + command );
            }
            out.println();
        }
        else
        {
            out.println( header + " " + availableCommandsAsString( server ) );
        }
        out.println( "Use " + getShortUsageString() + " for info about each command." );
    }

    /**
     * Lists apps available at the server.
     *
     * @param server
     *            the {@link ShellServer}.
     * @return a list of available commands a client can execute, whre the
     *         server is an {@link AppShellServer}.
     */
    public static synchronized Collection<String> getAvailableCommands(
        ShellServer server )
    {
        if ( availableCommands == null )
        {
            Collection<String> list = new ArrayList<>();
            // TODO Shouldn't trust the server to be an AbstractAppServer
            for ( String name : ( ( AbstractAppServer ) server )
                .getAllAvailableCommands() )
            {
                list.add( name );
            }
            availableCommands = list;
        }
        return availableCommands;
    }

    private static synchronized String availableCommandsAsString(
        ShellServer server )
    {
        StringBuffer commands = new StringBuffer();
        for ( String command : getAvailableCommands( server ) )
        {
            if ( commands.length() > 0 )
            {
                commands.append( " " );
            }
            commands.append( command );
        }
        return commands.toString();
    }
}
