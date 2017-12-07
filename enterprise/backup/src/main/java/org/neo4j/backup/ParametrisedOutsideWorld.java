/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.backup;

import java.io.Console;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;

/**
 * An outside world where you can pick and choose which input/output are dummies.
 */
class ParametrisedOutsideWorld implements OutsideWorld
{

    private final PrintStream stdout;
    private final PrintStream stderr;
    private final Console stdin;
    private final FileSystemAbstraction fileSystemAbstraction;

    ParametrisedOutsideWorld( StringBuilder stdout, StringBuilder stderr )
    {
        this( System.console(), streamFromBuilder( stdout ), streamFromBuilder( stderr ), new DefaultFileSystemAbstraction() );
    }

    private static OutputStream streamFromBuilder( StringBuilder stringBuilder )
    {
        return new OutputStream()
        {
            @Override
            public void write( int i ) throws IOException
            {
                stringBuilder.append( (char) i );
            }
        };
    }

    ParametrisedOutsideWorld( Console stdin, OutputStream stdout, OutputStream stderr, FileSystemAbstraction fileSystemAbstraction )
    {
        this.stdout = new PrintStream( stdout );
        this.stderr = new PrintStream( stderr );
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.stdin = stdin;
    }

    ParametrisedOutsideWorld( PrintStream stdout, PrintStream stderr )
    {
        this( System.console(), stdout, stderr, new DefaultFileSystemAbstraction() );
    }

    @Override
    public void stdOutLine( String text )
    {
        stdout.println( text );
    }

    @Override
    public void stdErrLine( String text )
    {
        stderr.println( text );
    }

    @Override
    public String readLine()
    {
        return stdin.readLine();
    }

    @Override
    public String promptLine( String fmt, Object... args )
    {
        return stdin.readLine( fmt, args );
    }

    @Override
    public char[] promptPassword( String fmt, Object... args )
    {
        return stdin.readPassword( fmt, args );
    }

    @Override
    public void exit( int status )
    {
        IOUtils.closeAllSilently( this );
    }

    @Override
    public void printStacktrace( Exception exception )
    {
        exception.printStackTrace( stderr );
    }

    @Override
    public FileSystemAbstraction fileSystem()
    {
        return fileSystemAbstraction;
    }

    @Override
    public PrintStream errorStream()
    {
        return stderr;
    }

    @Override
    public PrintStream outStream()
    {
        return stdout;
    }

    @Override
    public void close() throws IOException
    {
        fileSystemAbstraction.close();
    }
}
