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
package org.neo4j.codegen.source;

import java.io.IOException;
import java.net.URI;
import javax.tools.SimpleJavaFileObject;

import static javax.tools.JavaFileObject.Kind.SOURCE;

class JavaSourceFile extends SimpleJavaFileObject
{
    private final StringBuilder content;

    JavaSourceFile( URI uri, StringBuilder content )
    {
        super( uri, SOURCE );
        this.content = content;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + toUri() + "]";
    }

    @Override
    public CharSequence getCharContent( boolean ignoreEncodingErrors ) throws IOException
    {
        return content;
    }

    /**
     * Reads characters into an array.
     *
     * @param pos The position of this file to start reading from
     * @param cbuf Destination buffer
     * @param off Offset at which to start storing characters
     * @param len Maximum number of characters to read (> 0)
     * @return The number of characters read (0 if no characters remain)
     * @see java.io.Reader#read(char[], int, int)
     */
    public int read( int pos, char[] cbuf, int off, int len )
    {
        len = Math.min( content.length() - pos, len );
        content.getChars( pos, pos + len, cbuf, off );
        return len;
    }
}
