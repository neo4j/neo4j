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

import org.neo4j.kernel.impl.core.LastCommittedTxIdSetter;

public class AsyncZooKeeperLastCommittedTxIdSetter implements LastCommittedTxIdSetter
{
    private final Broker broker;
    private final Updater updater;

    public AsyncZooKeeperLastCommittedTxIdSetter( Broker broker )
    {
        this.broker = broker;
        this.updater = new Updater();
        this.updater.start();
    }

    public void setLastCommittedTxId( long txId )
    {
        updater.setTarget( txId );
    }
    
    private class Updater extends Thread
    {
        private volatile long targetTxId;
        private long lastUpdatedTxId;
        private boolean halted;
        
        @Override
        public void run()
        {
            while ( !halted )
            {
                long txId = targetTxId;
                if ( txId == lastUpdatedTxId )
                {
                    waitForAChange();
                    continue;
                }
                
                try
                {
                    broker.setLastCommittedTxId( txId );
                    lastUpdatedTxId = txId;
                }
                catch ( Exception e )
                {   // OK
                }
            }
        }
        
        private synchronized void setTarget( long txId )
        {
            targetTxId = txId;
            notify();
        }

        private synchronized void waitForAChange()
        {
            try
            {
                wait();
            }
            catch ( InterruptedException e )
            {
                Thread.interrupted();
            }
        }
        
        private synchronized void halt()
        {
            halted = true;
            notify();
        }
    }
    
    public void close()
    {
        updater.halt();
        try
        {
            updater.join();
        }
        catch ( InterruptedException e )
        {
            Thread.interrupted();
        }
    }
}
