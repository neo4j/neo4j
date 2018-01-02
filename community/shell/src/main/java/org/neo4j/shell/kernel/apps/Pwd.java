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
package org.neo4j.shell.kernel.apps;

import java.rmi.RemoteException;
import java.util.List;

import org.neo4j.helpers.Service;
import org.neo4j.shell.App;
import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. prints the current
 * working directory "walked by" the "cd" application.
 */
@Service.Implementation( App.class )
public class Pwd extends TransactionProvidingApp
{
    @Override
    public String getDescription()
    {
        return "Prints path to current node or relationship";
    }

    @Override
    protected Continuation exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException, RemoteException
    {
        String current = null;
        try
        {
            current = getDisplayName( getServer(), session, getCurrent( session ), false );
        }
        catch ( ShellException e )
        {
            current = getDisplayNameForNonExistent();
        }
        out.println( "Current is " + current );

        String path = stringifyPath( Cd.readCurrentWorkingDir( session ), session ) + current;
        if ( path.length() > 0 )
        {
            out.println( path );
        }
        return Continuation.INPUT_COMPLETE;
    }

    private String stringifyPath( List<TypedId> pathIds, Session session )
        throws ShellException
    {
        if ( pathIds.isEmpty() )
        {
            return "";
        }
        StringBuilder path = new StringBuilder();
        for ( TypedId id : pathIds )
        {
            String displayName;
            try
            {
                displayName = getDisplayName( getServer(), session, id, false );
            }
            catch ( ShellException e )
            {
                displayName = getDisplayNameForNonExistent();
            }
            path.append( displayName ).append( "-->" );
        }
        return path.toString();
    }
}