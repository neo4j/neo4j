/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.util.logging.Level;
import java.util.logging.Logger;

public class VisibleMigrationProgressMonitor implements MigrationProgressMonitor
{
    protected static final Logger logger = Logger
            .getLogger( MigrationProgressMonitor.class.getName() );
    private final PrintStream out;

    public VisibleMigrationProgressMonitor( PrintStream out )
    {
        this.out = out;
    }

    public void started()
    {
        String message = "Starting upgrade of database store files";
        out.println( message );
        logger.log( Level.INFO, message );
    }

    public void percentComplete( int percent )
    {
        out.print( "." );
        out.flush();
        if (percent % 10 == 0)
        {
            logger.log( Level.INFO, String.format("Store upgrade %d%% complete", percent) );
        }
    }

    public void finished()
    {
        String message = "Finished upgrade of database store files";
        out.println();
        out.println( message );
        logger.log( Level.INFO, message );
    }
}
