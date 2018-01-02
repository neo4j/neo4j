/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.tools.txlog;

import java.io.PrintStream;

/**
 * Handler that simply prints given number of inconsistencies to {@link PrintingInconsistenciesHandler#out} and throws
 * an exception if too many inconsistencies are found.
 */
class PrintingInconsistenciesHandler implements InconsistenciesHandler
{
    private static final int DEFAULT_NUMBER_OF_INCONSISTENCIES_TO_PRINT = 1024;

    private final int inconsistenciesToPrint;
    private int seenInconsistencies;
    private PrintStream out;

    PrintingInconsistenciesHandler( PrintStream out )
    {
        this( out, DEFAULT_NUMBER_OF_INCONSISTENCIES_TO_PRINT );
    }

    PrintingInconsistenciesHandler( PrintStream out, int inconsistenciesToPrint )
    {
        this.out = out;
        this.inconsistenciesToPrint = inconsistenciesToPrint;
    }

    @Override
    public void handle( LogRecord<?> committed, LogRecord<?> current )
    {
        out.println( "Before state: " + committed + " is inconsistent with after state: " + current );
        seenInconsistencies++;
        if ( seenInconsistencies >= inconsistenciesToPrint )
        {
            throw new RuntimeException( "Too many inconsistencies found" );
        }
    }
}
