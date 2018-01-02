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
package org.neo4j.commandline.admin;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;

import org.neo4j.io.fs.FileSystemAbstraction;

public class NullOutsideWorld implements OutsideWorld
{
    @Override
    public void stdOutLine( String text )
    {
    }

    @Override
    public void stdErrLine( String text )
    {
    }

    @Override
    public String readLine()
    {
        return "";
    }

    @Override
    public String promptLine( String fmt, Object... args )
    {
        return "";
    }

    @Override
    public char[] promptPassword( String fmt, Object... args )
    {
        return new char[0];
    }

    @Override
    public void exit( int status )
    {
    }

    @Override
    public void printStacktrace( Exception exception )
    {
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return null;
    }

    @Override
    public PrintStream errorStream()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public PrintStream outStream()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public InputStream inStream()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
