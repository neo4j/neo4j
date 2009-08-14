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
import java.util.List;

import org.neo4j.impl.shell.NeoApp;
import org.neo4j.impl.shell.apps.NodeOrRelationship.TypedId;
import org.neo4j.util.shell.AppCommandParser;
import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;

/**
 * Mimics the POSIX application with the same name, i.e. prints the current
 * working directory "walked by" the "cd" application.
 */
public class Pwd extends NeoApp
{
    @Override
    public String getDescription()
    {
        return "Prints path to current node or relatioship";
    }

    @Override
    protected String exec( AppCommandParser parser, Session session,
        Output out ) throws RemoteException
    {
        NodeOrRelationship current = this.getCurrent( session );
        out.println( "Current is " +
            getDisplayName( getNeoServer(), session, current ) );

        String path = stringifyPath( Cd.readPaths( session ), session );
        if ( path.length() > 0 )
        {
            out.println( path );
        }
        return null;
    }

    private String stringifyPath( List<TypedId> pathIds, Session session )
    {
        if ( pathIds.isEmpty() )
        {
            return "";
        }
        StringBuilder path = new StringBuilder();
        for ( TypedId id : pathIds )
        {
            path.append(
                getDisplayName( getNeoServer(), session, id ) ).append( "-->" );
        }
        return path.append( getDisplayNameForCurrent( session ) ).toString();
    }
}