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
package org.neo4j.shell;

import java.io.IOException;
import java.io.Writer;

public class OutputAsWriter extends Writer
{
    private final Output out;

    public OutputAsWriter( Output out )
    {
        this.out = out;
    }

    @Override
    public void write( char[] cbuf, int off, int len ) throws IOException
    {
        String string = String.valueOf( cbuf, off, len );
        int lastNewline = string.lastIndexOf( System.lineSeparator() );
        if ( lastNewline == -1 )
        {
            out.print( string );
        } else
        {
            out.println( string.substring( 0, lastNewline ) );
            out.print( string.substring( lastNewline + System.lineSeparator().length() ) );
        }
    }

    @Override
    public void flush() throws IOException
    {
    }

    @Override
    public void close() throws IOException
    {
    }
}
