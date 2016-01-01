/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.com;

import org.junit.Test;

import org.neo4j.kernel.impl.util.TestLogger;

public class LoggingResourcePoolMonitorTest
{
    @Test
    public void testUpdatedCurrentPeakSizeLogsOnlyOnChange() throws Exception
    {
        TestLogger logger = new TestLogger();
        LoggingResourcePoolMonitor monitor = new LoggingResourcePoolMonitor( logger );

        monitor.updatedCurrentPeakSize( 10 );
        logger.assertLogCallAtLevel( "DEBUG", 1 );
        logger.clear();

        monitor.updatedCurrentPeakSize( 10 );
        logger.assertNoDebugs();

        monitor.updatedCurrentPeakSize( 11 );
        logger.assertLogCallAtLevel( "DEBUG", 1 );
    }

    @Test
    public void testUpdatedTargetSizeOnlyOnChange() throws Exception
    {
        TestLogger logger = new TestLogger();
        LoggingResourcePoolMonitor monitor = new LoggingResourcePoolMonitor( logger );

        monitor.updatedTargetSize( 10 );
        logger.assertLogCallAtLevel( "DEBUG", 1 );
        logger.clear();

        monitor.updatedTargetSize( 10 );
        logger.assertNoDebugs();

        monitor.updatedTargetSize( 11 );
        logger.assertLogCallAtLevel( "DEBUG", 1 );
    }
}
