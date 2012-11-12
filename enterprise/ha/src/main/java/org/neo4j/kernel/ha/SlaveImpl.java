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
package org.neo4j.kernel.ha;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.kernel.GraphDatabaseAPI;

public class SlaveImpl implements Slave
{
    private final GraphDatabaseAPI graphDb;
    private final Broker broker;
    private final SlaveDatabaseOperations dbOperations;

    public SlaveImpl( GraphDatabaseAPI graphDb, Broker broker, SlaveDatabaseOperations dbOperations )
    {
        this.graphDb = graphDb;
        this.broker = broker;
        this.dbOperations = dbOperations;
    }

    @Override
    public Response<Void> pullUpdates( String resource, long upToAndIncludingTxId )
    {
        // Pull updates from the master
        dbOperations.receive( broker.getMaster().first().pullUpdates( dbOperations.getSlaveContext( 0 ) ) );
        return ServerUtil.packResponseWithoutTransactionStream( graphDb, RequestContext.EMPTY, null );
    }
    
    @Override
    public int getServerId()
    {
        return broker.getMyMachineId();
    }
}
