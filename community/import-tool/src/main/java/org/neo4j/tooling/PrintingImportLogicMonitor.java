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
package org.neo4j.tooling;

import java.io.PrintStream;

import org.neo4j.unsafe.impl.batchimport.ImportLogic;

class PrintingImportLogicMonitor implements ImportLogic.Monitor
{
    private final PrintStream out;
    private final PrintStream err;

    PrintingImportLogicMonitor( PrintStream out, PrintStream err )
    {
        this.out = out;
        this.err = err;
    }

    @Override
    public void doubleRelationshipRecordUnitsEnabled()
    {
        out.println( "Will use double record units for all relationships" );
    }

    @Override
    public void mayExceedNodeIdCapacity( long capacity, long estimatedCount )
    {
        err.printf( "WARNING: estimated number of relationships %d may exceed capacity %d of selected record format%n",
                estimatedCount, capacity );
    }

    @Override
    public void mayExceedRelationshipIdCapacity( long capacity, long estimatedCount )
    {
        err.printf( "WARNING: estimated number of nodes %d may exceed capacity %d of selected record format%n",
                estimatedCount, capacity );
    }
}
