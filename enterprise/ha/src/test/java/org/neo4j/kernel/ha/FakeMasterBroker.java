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

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.com.Protocol;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.zookeeper.Machine;

public class FakeMasterBroker extends AbstractBroker
{
    private final Collection<Slave> slaves = new ArrayList<Slave>();
    
    public FakeMasterBroker( Config conf )
    {
        super( conf );
    }

    public Machine getMasterMachine()
    {
        return new Machine( getMyMachineId(), 0, 1, -1, null, 0 );
    }

    public Pair<Master, Machine> getMaster()
    {
        return Pair.<Master, Machine>of( null, new Machine( getMyMachineId(),
                0, 1, -1, null, 0 ) );
    }

    public Pair<Master, Machine> getMasterReally( boolean allowChange )
    {
        return Pair.<Master, Machine>of( null, new Machine( getMyMachineId(),
                0, 1, -1, null, 0 ) );
    }

    public boolean iAmMaster()
    {
        return getMyMachineId() == 0;
    }

    public Object instantiateMasterServer( GraphDatabaseAPI graphDb )
    {
        int timeOut = config.isSet( HaSettings.lock_read_timeout ) ? config.getInteger( HaSettings.lock_read_timeout ) : config
            .getInteger( HaSettings.read_timeout );
        return new MasterServer( new MasterImpl( graphDb, timeOut ), Protocol.PORT, graphDb.getMessageLog(),
                config.getInteger( HaSettings.max_concurrent_channels_per_slave ),
                timeOut, TxChecksumVerifier.ALWAYS_MATCH );
    }
    
    public Object instantiateSlaveServer( GraphDatabaseAPI graphDb, SlaveDatabaseOperations ops )
    {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Slave[] getSlaves()
    {
        return slaves.toArray( new Slave[slaves.size()] );
    }
    
    public void addSlave( Slave slave )
    {
        slaves.add( slave );
    }
    
    @Override
    public void shutdown()
    {
        for ( Slave slave : slaves )
            ((SlaveClient)slave).shutdown();
    }
}
