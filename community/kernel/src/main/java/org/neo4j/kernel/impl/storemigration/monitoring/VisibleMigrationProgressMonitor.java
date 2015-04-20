/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration.monitoring;

import java.io.PrintStream;

import org.neo4j.kernel.impl.util.StringLogger;

import static java.lang.String.format;

public class VisibleMigrationProgressMonitor implements MigrationProgressMonitor
{
    private final StringLogger logger;
    private final PrintStream out;

    public VisibleMigrationProgressMonitor( StringLogger logger, PrintStream out )
    {
        this.logger = logger;
        this.out = out;
    }

    @Override
    public void started()
    {
        String message = "Starting upgrade of database store files";
        out.println( message );
        logger.info( message );
    }

    @Override
    public void percentComplete( int percent )
    {
        out.print( "." );
        if (percent % 10 == 0)
        {
            logger.info( format( "Store upgrade %d%% complete", percent ) );
            out.println( " " + percent + "%" );
        }
        out.flush();
    }

    @Override
    public void finished()
    {
        String message = "Finished upgrade of database store files";
        out.println( message );
        logger.info( message );
    }
}
