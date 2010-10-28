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

package org.neo4j.kernel.ha.zookeeper;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.MasterServer;
import org.neo4j.kernel.ha.ResponseReceiver;

public class ZooKeeperBroker extends AbstractBroker
{
    private final ZooClient zooClient;
    private final String haServer;
    private final int machineId;
    
    public ZooKeeperBroker( String storeDir, int machineId, String zooKeeperServers, 
            String haServer, ResponseReceiver receiver )
    {
        super( machineId, storeDir );
        this.machineId = machineId;
        this.haServer = haServer;
        NeoStoreUtil store = new NeoStoreUtil( storeDir ); 
        this.zooClient = new ZooClient( zooKeeperServers, machineId, store.getCreationTime(),
                store.getStoreId(), store.getLastCommittedTx(), receiver, haServer, storeDir );
    }
    
    public Pair<Master, Machine> getMaster()
    {
        return zooClient.getCachedMaster();
    }
    
    public Pair<Master, Machine> getMasterReally()
    {
        return zooClient.getMasterFromZooKeeper( true );
    }
    
    public Machine getMasterExceptMyself()
    {
        Map<Integer, Machine> machines = zooClient.getAllMachines( true );
        machines.remove( this.machineId );
        return zooClient.getMasterBasedOn( machines.values() );
    }
    
    public Object instantiateMasterServer( GraphDatabaseService graphDb )
    {
        MasterServer server = new MasterServer( new MasterImpl( graphDb ),
                Machine.splitIpAndPort( haServer ).other(), getStoreDir() );
        return server;
    }

    public void setLastCommittedTxId( long txId )
    {
        zooClient.setCommittedTx( txId );
    }
    
    public boolean iAmMaster()
    {
        return zooClient.getCachedMaster().other().getMachineId() == getMyMachineId();
    }
    
    public void shutdown()
    {
        zooClient.shutdown();
    }
    
    public void rebindMaster()
    {
        zooClient.setDataChangeWatcher( ZooClient.MASTER_REBOUND_CHILD, machineId );
    }
}
