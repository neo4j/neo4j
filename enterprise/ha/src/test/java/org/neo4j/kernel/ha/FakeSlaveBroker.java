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

import org.neo4j.com.Client;
import org.neo4j.com.Protocol;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.impl.util.StringLogger;

public class FakeSlaveBroker extends AbstractBroker
{
    private final Master master;
    
    public FakeSlaveBroker( Master master, int masterMachindId, Config config )
    {
        super( config );
        this.master = master;
    }

    public FakeSlaveBroker( StringLogger log, int masterMachineId, Config config )
    {
        this( new MasterClient( "localhost",
                Protocol.PORT, log, storeId, Client.ConnectionLostHandler.NO_ACTION, Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS,
                Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS,
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT ), masterMachineId, config );
    }

    public Pair<Master, Machine> getMaster()
    {
        return Pair.<Master, Machine>of( master, Machine.NO_MACHINE );
    }

    public Pair<Master, Machine> getMasterReally( boolean allowChange )
    {
        return Pair.<Master, Machine>of( master, Machine.NO_MACHINE );
    }

    public boolean iAmMaster()
    {
        return false;
    }

    public Object instantiateMasterServer( GraphDatabaseAPI graphDb )
    {
        throw new UnsupportedOperationException();
    }
}
