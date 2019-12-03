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
package org.neo4j.index.internal.gbptree;

import java.io.PrintStream;

@SuppressWarnings( {"unused", "WeakerAccess"} )
public final class PrintConfig
{
    private PrintStream printStream;
    private boolean printValues;
    private boolean printPosition;
    private boolean printState;
    private boolean printHeader;
    private boolean printFreelist;
    private boolean printOffload;

    private PrintConfig()
    {
        printStream = System.out;
    }

    /**
     * Print to System.out and exclude any extra information,
     * use builder methods to switch them on.
     */
    public static PrintConfig defaults()
    {
        return new PrintConfig();
    }

    public PrintConfig printStream( PrintStream out )
    {
        this.printStream = out;
        return this;
    }

    public PrintConfig printValue()
    {
        this.printValues = true;
        return this;
    }

    public PrintConfig printPosition()
    {
        this.printPosition = true;
        return this;
    }
    public PrintConfig printState()
    {
        this.printState = true;
        return this;
    }
    public PrintConfig printHeader()
    {
        this.printHeader = true;
        return this;
    }
    public PrintConfig printFreelist()
    {
        this.printFreelist = true;
        return this;
    }
    public PrintConfig printOffload()
    {
        this.printOffload = true;
        return this;
    }

    PrintStream getPrintStream()
    {
        return printStream;
    }

    boolean getPrintValues()
    {
        return printValues;
    }

    boolean getPrintPosition()
    {
        return printPosition;
    }

    boolean getPrintState()
    {
        return printState;
    }

    boolean getPrintHeader()
    {
        return printHeader;
    }

    boolean getPrintFreelist()
    {
        return printFreelist;
    }

    boolean getPrintOffload()
    {
        return printOffload;
    }
}
