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
package org.neo4j.com;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Test;
import org.neo4j.logging.Log;

public class LoggingResourcePoolMonitorTest
{
    @Test
    public void testUpdatedCurrentPeakSizeLogsOnlyOnChange() throws Exception
    {
        Log log = mock( Log.class );
        LoggingResourcePoolMonitor monitor = new LoggingResourcePoolMonitor( log );

        monitor.updatedCurrentPeakSize( 10 );
        verify( log, times( 1 ) ).debug( anyString() );

        monitor.updatedCurrentPeakSize( 10 );
        verify( log, times( 1 ) ).debug( anyString() );

        monitor.updatedCurrentPeakSize( 11 );
        verify( log, times( 2 ) ).debug( anyString() );
    }

    @Test
    public void testUpdatedTargetSizeOnlyOnChange() throws Exception
    {
        Log log = mock( Log.class );
        LoggingResourcePoolMonitor monitor = new LoggingResourcePoolMonitor( log );

        monitor.updatedTargetSize( 10 );
        verify( log, times( 1 ) ).debug( anyString() );

        monitor.updatedTargetSize( 10 );
        verify( log, times( 1 ) ).debug( anyString() );

        monitor.updatedTargetSize( 11 );
        verify( log, times( 2 ) ).debug( anyString() );
    }
}
