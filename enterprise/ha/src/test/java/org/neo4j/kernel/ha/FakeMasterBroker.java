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

import java.util.Map;

import org.neo4j.com.Protocol;
import org.neo4j.com.TxChecksumVerifier;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.HaConfig;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class FakeMasterBroker extends AbstractBroker
{
    private Map<String, String> config;
    private StoreId storeId = new StoreId();

    public FakeMasterBroker( int myMachineId, AbstractGraphDatabase graphDb,
            Map<String, String> config )
    {
        super( myMachineId, graphDb );
        this.config = config;
    }

    @Override
    public StoreId getClusterStoreId( boolean firstTime )
    {
        return storeId; // Master will always win
    }

    public Machine getMasterMachine()
    {
        return new Machine( getMyMachineId(), 0, 1, -1, null );
    }

    public Pair<Master, Machine> getMaster()
    {
        return Pair.<Master, Machine>of( null, new Machine( getMyMachineId(),
                0, 1, -1, null ) );
        // throw new UnsupportedOperationException( "I am master" );
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

    public Object instantiateMasterServer( AbstractGraphDatabase graphDb )
    {
        return new MasterServer( new MasterImpl( graphDb, config ), Protocol.PORT, graphDb.getMessageLog(),
                HaConfig.getMaxConcurrentTransactionsOnMasterFromConfig( config ),
                HaConfig.getClientLockReadTimeoutFromConfig( config ), TxChecksumVerifier.ALWAYS_MATCH );
    }
}
