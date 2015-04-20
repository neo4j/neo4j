/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ext.monitorlogging;

import org.junit.Test;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.neo4j.kernel.impl.util.TestLogger.LogCall.warn;

public class MonitorLoggingAcceptanceTest
{

    @Test
    public void shouldBeAbleToLoadPropertiesFromAFile() throws InterruptedException
    {
        TestLogging logging = new TestLogging();
        GraphDatabaseAPI dbAPI = (GraphDatabaseAPI) new TestGraphDatabaseFactory( logging ).newImpermanentDatabase();

        Monitors monitors = dbAPI.getDependencyResolver().resolveDependency( Monitors.class );
        AMonitor aMonitor = monitors.newMonitor( AMonitor.class );
        aMonitor.doStuff();

        logging.getMessagesLog( AMonitor.class ).assertExactly( warn( "doStuff()" ) );
    }
}
