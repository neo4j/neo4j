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
package org.neo4j.test.randomized;

import java.io.PrintStream;

public class PrintStreamLinePrinter implements LinePrinter
{
    private final PrintStream out;
    private final int indentation;

    public PrintStreamLinePrinter( PrintStream out )
    {
        this( out, 0 );
    }
    
    public PrintStreamLinePrinter( PrintStream out, int indentation )
    {
        this.out = out;
        this.indentation = indentation;
    }
    
    @Override
    public void println( String line )
    {
        for ( int i = 0; i < indentation; i++ )
        {
            out.print( "    " );
        }
        out.println( line );
    }

    @Override
    public LinePrinter indent()
    {
        return new PrintStreamLinePrinter( out, indentation+1 );
    }
}
