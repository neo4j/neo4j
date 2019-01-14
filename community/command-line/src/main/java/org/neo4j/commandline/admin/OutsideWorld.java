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

import java.io.Closeable;
import java.io.InputStream;
import java.io.PrintStream;

import org.neo4j.io.fs.FileSystemAbstraction;

public interface OutsideWorld extends Closeable
{
    void stdOutLine( String text );

    void stdErrLine( String text );

    /**
     * @see java.io.Console#readLine()
     */
    String readLine();

    /**
     * @see java.io.Console#readLine(String, Object...)
     */
    String promptLine( String fmt, Object... args );

    /**
     * It is strongly advised that the return character array is overwritten as soon as the password has been processed,
     * to avoid having it linger in memory any longer than strictly necessary.
     *
     * @see java.io.Console#readPassword(String, Object...)
     */
    char[] promptPassword( String fmt, Object... args );

    void exit( int status );

    void printStacktrace( Exception exception );

    FileSystemAbstraction fileSystem();

    PrintStream errorStream();

    PrintStream outStream();

    InputStream inStream();
}
