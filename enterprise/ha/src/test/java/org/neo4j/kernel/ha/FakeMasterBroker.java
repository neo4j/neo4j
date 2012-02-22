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
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseSPI;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.ha.zookeeper.ZooClient;

public class FakeMasterBroker extends AbstractBroker
{
    private ZooClient.Configuration zooClientConfig;

    public FakeMasterBroker( Configuration conf, ZooClient.Configuration zooClientConfig )
    {
        super( conf );
        this.zooClientConfig = zooClientConfig;
    }

    public Machine getMasterMachine()
    {
        return new Machine( getMyMachineId(), 0, 1, -1, null );
    }

    public Pair<Master, Machine> getMaster()
    {
        return Pair.<Master, Machine>of( null, new Machine( getMyMachineId(),
                0, 1, -1, null ) );
    }

    public Pair<Master, Machine> getMasterReally( boolean allowChange )
    {
        return Pair.<Master, Machine>of( null, new Machine( getMyMachineId(),
                0, 1, -1, null ) );
    }

    public boolean iAmMaster()
    {
        return getMyMachineId() == 0;
    }

    public Object instantiateMasterServer( GraphDatabaseSPI graphDb )
    {
        return new MasterServer( new MasterImpl( graphDb, zooClientConfig.lock_read_timeout( Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS ) ),
                Protocol.PORT, graphDb.getMessageLog(), zooClientConfig.max_concurrent_channels_per_slave( Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT ),
                zooClientConfig.lock_read_timeout( Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS ), TxChecksumVerifier.ALWAYS_MATCH );
    }
}
