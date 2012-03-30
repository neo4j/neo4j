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

import org.neo4j.helpers.Pair;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

public abstract class AbstractBroker implements Broker
{
    private static final StoreId storeId = new StoreId();
    protected Config config;

    public AbstractBroker( Config config)
    {
        this.config = config;
    }

    public void setLastCommittedTxId( long txId )
    {
        // Do nothing
    }

    protected Config getConfig()
    {
        return config;
    }

    public int getMyMachineId()
    {
        return config.getInteger( HaSettings.server_id );
    }

    @Override
    public void notifyMasterChange( Machine newMaster )
    {
        // Do nothing
    }

    public void shutdown()
    {
        // Do nothing
    }

    public void restart()
    {
        // Do nothing
    }

    public void start()
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

    public void setConnectionInformation( KernelData kernel )
    {
    }

    public ConnectionInformation getConnectionInformation( int machineId )
    {
        throw new UnsupportedOperationException( getClass().getName()
                                                 + " does not support ConnectionInformation" );
    }

    public ConnectionInformation[] getConnectionInformation()
    {
        throw new UnsupportedOperationException( getClass().getName()
                                                 + " does not support ConnectionInformation" );
    }

    public StoreId getClusterStoreId()
    {
        return storeId;
    }

    @Override
    public void logStatus( StringLogger msgLog )
    {
        // defult: log nothing
    }

    @Override
    public Pair<Master, Machine> bootstrap()
    {
        return getMasterReally( true );
    }
}
