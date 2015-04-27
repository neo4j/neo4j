/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.management.impl.jconsole;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;

import javax.swing.JLabel;
import javax.swing.JPanel;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.management.RemoteConnection;

class DataBrowser extends Widget
{
    private final GraphDatabaseService graphDb;

    DataBrowser( RemoteConnection remote ) throws ClassNotFoundException, SecurityException,
            NoSuchMethodException, IllegalAccessException, IllegalArgumentException
    {
        Class<?> jmxTarget = Class.forName( "org.neo4j.remote.transports.JmxTarget" );
        Method connect = jmxTarget.getMethod( "connectGraphDatabase", RemoteConnection.class );
        try
        {
            this.graphDb = (GraphDatabaseService) connect.invoke( null, remote );
        }
        catch ( InvocationTargetException e )
        {
            throw launderRuntimeException( e.getTargetException() );
        }
    }

    static RuntimeException launderRuntimeException( Throwable exception )
    {
        if ( exception instanceof RuntimeException )
        {
            return (RuntimeException) exception;
        }
        else if ( exception instanceof Error )
        {
            throw (Error) exception;
        }
        else
        {
            throw new RuntimeException( "Unexpected Exception!", exception );
        }
    }

    @Override
    void populate( JPanel view )
    {
        view.add( new JLabel( "Place holder for the Neo4j data viewer" ) );
    }

    @Override
    void dispose()
    {
        graphDb.shutdown();
    }

    @Override
    void update( Collection<UpdateEvent> result )
    {
        // TODO tobias: Implement update() [Nov 30, 2010]
    }
}
