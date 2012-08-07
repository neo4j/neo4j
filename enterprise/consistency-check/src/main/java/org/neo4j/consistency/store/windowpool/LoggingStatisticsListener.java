/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.consistency.store.windowpool;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;

import org.neo4j.helpers.Format;

public class LoggingStatisticsListener implements MappingStatisticsListener
{
    private final PrintWriter logWriter;

    public LoggingStatisticsListener( String fileName ) throws FileNotFoundException
    {
        this.logWriter = new PrintWriter( new FileOutputStream( fileName, true ) );
    }

    @Override
    public void onStatistics( String storeFileName, int acquiredPages, int mappedPages, long
            samplePeriod )
    {
        logWriter.printf( "%s: In %s: %d pages acquired, %d pages mapped (%.2f%%) in %d ms%n",
                Format.date(), storeFileName, acquiredPages, mappedPages,
                (100.0 * mappedPages) / acquiredPages, samplePeriod );
        logWriter.flush();
    }
}
