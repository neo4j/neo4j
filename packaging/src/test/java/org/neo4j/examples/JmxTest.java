/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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


package org.neo4j.examples;

import static org.junit.Assert.assertTrue;

import java.util.Date;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.management.Kernel;

public class JmxTest
{
    @Test
    public void readJmxProperties()
    {
        GraphDatabaseService graphDbService = new EmbeddedGraphDatabase(
                "target/jmx-db" );
        Date startTime = getStartTimeFromManagementBean( graphDbService );
        Date now = new Date();
        System.out.println( startTime + " " + now );
        assertTrue( startTime.before( now ) || startTime.equals( now ) );
    }

    // START SNIPPET: getStartTime
    private static Date getStartTimeFromManagementBean(
            GraphDatabaseService graphDbService )
    {
        // use EmbeddedGraphDatabase to access management beans
        EmbeddedGraphDatabase graphDb = (EmbeddedGraphDatabase) graphDbService;
        Kernel kernel = graphDb.getManagementBean( Kernel.class );
        Date startTime = kernel.getKernelStartTime();
        return startTime;
    }
    // END SNIPPET: getStartTime
}
