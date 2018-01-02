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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.shell.Output;

public class CollectingOutput extends UnicastRemoteObject implements Output, Serializable, Iterable<String>
{
    private static final long serialVersionUID = 1L;

    private transient StringWriter stringWriter = new StringWriter();
    private transient PrintWriter allLinesAsOne = new PrintWriter( stringWriter );

    private final List<String> lines = new ArrayList<String>();

    private String ongoingLine = "";

    public CollectingOutput() throws RemoteException
    {
        super();
    }
    
    @Override
    public Appendable append( CharSequence csq, int start, int end )
            throws IOException
    {
        this.print( RemoteOutput.asString( csq ).substring( start, end ) );
        return this;
    }

    @Override
    public Appendable append( char c ) throws IOException
    {
        this.print( c );
        return this;
    }

    @Override
    public Appendable append( CharSequence csq ) throws IOException
    {
        this.print( RemoteOutput.asString( csq ) );
        return this;
    }

    @Override
    public void println( Serializable object ) throws RemoteException
    {
        print( object );
        println();
    }

    @Override
    public void println() throws RemoteException
    {
        lines.add( ongoingLine );
        allLinesAsOne.println( ongoingLine );
        ongoingLine = "";
    }

    @Override
    public void print( Serializable object ) throws RemoteException
    {
        String string = object.toString();
        int index = 0;
        while ( true )
        {
            index = string.indexOf( System.lineSeparator(), index );
            if ( index < 0 )
            {
                ongoingLine += string;
                break;
            }
            
            String part = string.substring( 0, index );
            ongoingLine += part;
            println();
            
            string = string.substring( index + System.lineSeparator().length(), string.length() );
        }
    }
    
    @Override
    public Iterator<String> iterator()
    {
        return lines.iterator();
    }
    
    public String asString()
    {
        try
        {
            allLinesAsOne.flush();
            stringWriter.flush();
            return stringWriter.getBuffer().toString();
        }
        finally
        {
            clear();
        }
    }
    
    private void clear()
    {
        lines.clear();
        ongoingLine = "";
        stringWriter = new StringWriter();
        allLinesAsOne = new PrintWriter( stringWriter );
    }
}
