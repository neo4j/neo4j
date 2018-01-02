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
package org.neo4j.shell.impl;

import java.rmi.RemoteException;
import java.util.List;

import jline.console.completer.Completer;
import jline.console.completer.StringsCompleter;

import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.TabCompletion;

@SuppressWarnings("UnusedDeclaration")
/**
 * This class is instantiated by reflection (in {@link JLineConsole#newConsoleOrNullIfNotFound}) in order to ensure
 * that there is no hard dependency on jLine and the console can run in degraded form without it.
 */
class ShellTabCompleter implements Completer
{
    private final ShellClient client;

    private long timeWhenCached;
    private Completer appNameCompleter;

    public ShellTabCompleter( ShellClient client )
    {
        this.client = client;
    }

    public int complete( String buffer, int cursor, List candidates )
    {
        if ( buffer == null || buffer.length() == 0 )
        {
            return cursor;
        }

        try
        {
            if ( buffer.contains( " " ) )
            {
                TabCompletion completion = client.getServer().tabComplete( client.getId(), buffer.trim() );
                cursor = completion.getCursor();
                //noinspection unchecked
                candidates.addAll( completion.getCandidates() );
            }
            else
            {
                // Complete the app name
                return getAppNameCompleter().complete( buffer, cursor, candidates );
            }
        }
        catch ( RemoteException e )
        {
            // TODO Throw something?
            e.printStackTrace();
        }
        catch ( ShellException e )
        {
            // TODO Throw something?
            e.printStackTrace();
        }
        return cursor;
    }

    private Completer getAppNameCompleter() throws RemoteException
    {
        if ( timeWhenCached != client.timeForMostRecentConnection() )
        {
            timeWhenCached = client.timeForMostRecentConnection();
            appNameCompleter = new StringsCompleter( client.getServer().getAllAvailableCommands() );
        }
        return this.appNameCompleter;
    }
}
