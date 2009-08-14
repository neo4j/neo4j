/*
 * Copyright (c) 2002-2009 "Neo Technology,"
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
package org.neo4j.impl.shell.apps;

import java.rmi.RemoteException;

import org.neo4j.impl.shell.NeoApp;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

/**
 * Mimics the POSIX application with the same name, i.e. removes a property from
 * a node or relationship. It could also (regarding POSIX) delete nodes,
 * but it doesn't.
 */
public class Rm extends NeoApp
{
    @Override
    public String getDescription()
    {
        return "Removes a property from the current node or relationship.\n" +
            "Usage: rm <key>";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session,
        Output out ) throws ShellException
    {
        if ( parser.arguments().isEmpty() )
        {
            throw new ShellException( "Must supply the property key to " +
                "remove, like: rm title" );
        }

        try
        {
            String key = parser.arguments().get( 0 );
            NodeOrRelationship thing = getCurrent( session );
            if ( thing.removeProperty( key ) == null )
            {
                out.println( "Property '" + key + "' not found" );
            }
            return null;
        }
        catch ( RemoteException e )
        {
            throw new ShellException( e );
        }
    }
}