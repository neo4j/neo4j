/*
 * Copyright (c) 2002-2008 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.shell.apps;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.regex.Pattern;

import org.neo4j.shell.AbstractApp;
import org.neo4j.shell.AbstractAppServer;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.AppShellServer;
import org.neo4j.shell.ClassLister;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;

/**
 * Prints a short manual for an {@link App}.
 */
public class Man extends AbstractApp
{
    private static Collection<String> availableCommands;

    public String execute( AppCommandParser parser, Session session,
        Output out ) throws ShellException
    {
        try
        {
            if ( parser.arguments().size() == 0 )
            {
                out.println( getHelpString( getServer() ) );
                return null;
            }

            App app = this.getApp( parser );
            out.println( "" );
            out.println( this.fixDesciption( app.getDescription() ) );
            println( out, "" );
            boolean hasOptions = false;
            for ( String option : app.getAvailableOptions() )
            {
                hasOptions = true;
                String description = this.fixDesciption(
                    app.getDescription( option ) );
                String[] descriptionLines = description.split(
                    Pattern.quote( "\n" ) );
                OptionValueType type = app.getOptionValueType( option );
                for ( int i = 0; i < descriptionLines.length; i++ )
                {
                    String line = "";
                    if ( i == 0 )
                    {
                        line = "-" + option;
                    }
                    line += "\t ";
                    line += descriptionLines[ i ];
                    if ( i == descriptionLines.length - 1 )
                    {
                        // line += type.getDescription();
                    }

                    println( out, line );
                }
            }
            if ( hasOptions )
            {
                println( out, "" );
            }
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
        return null;
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

    private App getApp( AppCommandParser parser ) throws ShellException
    {
        String appName = parser.arguments().get( 0 );
        try
        {
            App app = this.getServer().findApp( appName );
            if ( app == null )
            {
                throw new ShellException( "No manual entry for '" + appName +
                    "'" );
            }
            return app;
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
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
     * @param server
     *            the server to ask for
     * @return the short introductory help string.
     */
    public static String getHelpString( ShellServer server )
    {
        return "Available commands: " + availableCommandsAsString( server ) +
            "\n" + "Use " + getShortUsageString() +
            " for info about each command.";
    }

    /**
     * Uses {@link ClassLister} to list apps available at the server.
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
            Collection<String> list = new ArrayList<String>();
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
