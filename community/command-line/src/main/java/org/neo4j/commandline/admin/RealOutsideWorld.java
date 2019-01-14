/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;

public class RealOutsideWorld implements OutsideWorld
{
    FileSystemAbstraction fileSystemAbstraction = new DefaultFileSystemAbstraction();
    private final InputStream in;
    private final PrintStream out;
    private final PrintStream err;

    public RealOutsideWorld()
    {
        this( System.out, System.err, System.in );
    }

    public RealOutsideWorld( PrintStream out, PrintStream err, InputStream inStream )
    {
        this.in = inStream;
        this.out = out;
        this.err = err;
    }

    @Override
    public void stdOutLine( String text )
    {
        out.println( text );
    }

    @Override
    public void stdErrLine( String text )
    {
        err.println( text );
    }

    @Override
    public String readLine()
    {
        return System.console().readLine();
    }

    @Override
    public String promptLine( String fmt, Object... args )
    {
        return System.console().readLine( fmt, args );
    }

    @Override
    public char[] promptPassword( String fmt, Object... args )
    {
        return System.console().readPassword( fmt, args );
    }

    @Override
    public void exit( int status )
    {
        IOUtils.closeAllSilently( this );
        System.exit( status );
    }

    @Override
    public void printStacktrace( Exception exception )
    {
        exception.printStackTrace();
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return fileSystemAbstraction;
    }

    @Override
    public PrintStream errorStream()
    {
        return err;
    }

    @Override
    public void close() throws IOException
    {
        fileSystemAbstraction.close();
    }

    @Override
    public PrintStream outStream()
    {
        return out;
    }

    @Override
    public InputStream inStream()
    {
        return in;
    }
}
