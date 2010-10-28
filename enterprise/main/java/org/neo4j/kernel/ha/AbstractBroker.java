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

package org.neo4j.kernel.ha;

import java.util.Map;

import org.neo4j.kernel.ha.zookeeper.Machine;

public abstract class AbstractBroker implements Broker
{
    private final int myMachineId;
    private final String storeDir;

    public AbstractBroker( int myMachineId, String storeDir )
    {
        this.myMachineId = myMachineId;
        this.storeDir = storeDir;
    }
    
    public void setLastCommittedTxId( long txId )
    {
        // Do nothing
    }
    
    public int getMyMachineId()
    {
        return this.myMachineId;
    }
    
    public String getStoreDir()
    {
        return storeDir;
    }
    
    public void shutdown()
    {
        // Do nothing
    }

    public Machine getMasterExceptMyself()
    {
        throw new UnsupportedOperationException();
    }
    
    public void rebindMaster()
    {
        // Do nothing
    }
    
    public static BrokerFactory wrapSingleBroker( final Broker broker )
    {
        return new BrokerFactory()
        {
            public Broker create( String storeDir, Map<String, String> graphDbConfig )
            {
                return broker;
            }
        };
    }
}
