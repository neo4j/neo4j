/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.dbapi.impl;

import org.neo4j.bolt.dbapi.BoltGraphDatabaseServiceSPI;
import org.neo4j.bolt.dbapi.BoltGraphDatabaseManagementServiceSPI;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.time.SystemNanoClock;

public class BoltKernelDatabaseManagementServiceProvider implements BoltGraphDatabaseManagementServiceSPI
{
    private final DatabaseManagementService managementService;
    private final SystemNanoClock clock;

    public BoltKernelDatabaseManagementServiceProvider( DatabaseManagementService managementService, SystemNanoClock clock )
    {
        this.managementService = managementService;
        this.clock = clock;
    }

    @Override
    public BoltGraphDatabaseServiceSPI database( String databaseName ) throws DatabaseNotFoundException, UnavailableException
    {
        GraphDatabaseFacade databaseFacade = (GraphDatabaseFacade) managementService.database( databaseName );
        return new BoltKernelGraphDatabaseServiceProvider( databaseFacade, clock, databaseName );
    }
}
