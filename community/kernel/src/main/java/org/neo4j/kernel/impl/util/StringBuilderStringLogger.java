/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.logging.LogMarker;

/** Does what it says on the box. */
public class StringBuilderStringLogger extends StringLogger
{
    private final StringBuilder sb;

    public StringBuilderStringLogger( StringBuilder sb )
    {
        this.sb = sb;
    }

    @Override
    public void logLongMessage( String msg, Visitor<LineLogger, RuntimeException> source, boolean flush )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void logMessage( String msg, boolean flush )
    {
        sb.append( msg ).append( "\n" );
    }

    @Override
    public void logMessage( String msg, LogMarker marker )
    {
        sb.append( msg ).append( "\n" );
    }

    @Override
    public void logMessage( String msg, Throwable cause, boolean flush )
    {
        sb.append( msg ).append( "\n" );
    }

    @Override
    public void addRotationListener( Runnable listener )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void flush()
    {

    }

    @Override
    public void close()
    {

    }

    @Override
    protected void logLine( String line )
    {
        sb.append( line ).append( "\n" );
    }
}
