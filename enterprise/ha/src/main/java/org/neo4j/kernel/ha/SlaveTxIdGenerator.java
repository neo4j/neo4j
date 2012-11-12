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

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import javax.transaction.TransactionManager;

import org.neo4j.com.ComException;
import org.neo4j.com.Response;
import org.neo4j.com.SlaveContext;
import org.neo4j.com.SlaveContext.Tx;
import org.neo4j.com.TxExtractor;
import org.neo4j.kernel.ha.zookeeper.ZooKeeperException;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.transaction.xaframework.LogBuffer;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

public class SlaveTxIdGenerator implements TxIdGenerator
{
    public static class SlaveTxIdGeneratorFactory implements TxIdGeneratorFactory
    {
        private final Broker broker;
        private final ResponseReceiver receiver;

        public SlaveTxIdGeneratorFactory( Broker broker, ResponseReceiver receiver )
        {
            this.broker = broker;
            this.receiver = receiver;
        }

        public TxIdGenerator create( TransactionManager txManager )
        {
            return new SlaveTxIdGenerator( broker, receiver, txManager );
        }
    }

    private final Broker broker;
    private final ResponseReceiver receiver;
    private final TxManager txManager;

    public SlaveTxIdGenerator( Broker broker, ResponseReceiver receiver,
            TransactionManager txManager )
    {
        this.broker = broker;
        this.receiver = receiver;
        this.txManager = (TxManager) txManager;
    }

    public long generate( final XaDataSource dataSource, final int identifier )
    {
        try
        {
            final int eventIdentifier = txManager.getEventIdentifier();
            Response<Long> response = broker.getMaster().first().commitSingleResourceTransaction(
                    onlyForThisDataSource( receiver.getSlaveContext( eventIdentifier ), dataSource ),
                    dataSource.getName(), new TxExtractor()
                    {
                        public void extract( LogBuffer buffer )
                        {
                            try
                            {
                                dataSource.getPreparedTransaction( identifier, buffer );
                            }
                            catch ( IOException e )
                            {
                                throw new RuntimeException( e );
                            }
                        }

                        public ReadableByteChannel extract()
                        {
                            try
                            {
                                return dataSource.getPreparedTransaction( identifier );
                            }
                            catch ( IOException e )
                            {
                                throw new RuntimeException( e );
                            }
                        }
                    });
            return receiver.receive( response );
        }
        catch ( ZooKeeperException e )
        {
            receiver.newMaster( e );
            throw e;
        }
        catch ( ComException e )
        {
            receiver.newMaster( e );
            throw e;
        }
    }

    @SuppressWarnings( "unchecked" )
    private SlaveContext onlyForThisDataSource( SlaveContext slaveContext, XaDataSource dataSource )
    {
        Tx txForDs = null;
        for ( Tx tx : slaveContext.lastAppliedTransactions() )
        {
            if ( tx.getDataSourceName().equals( dataSource.getName() ) )
            {
                txForDs = tx;
                break;
            }
        }
        if ( txForDs == null )
        {   // Should not be able to happen
            throw new RuntimeException( "Apparently " + slaveContext +
                    " didn't have the XA data source we are commiting (" + dataSource.getName() + ")" );
        }
        return new SlaveContext( slaveContext.getSessionId(), slaveContext.machineId(),
                slaveContext.getEventIdentifier(), new Tx[] {txForDs}, slaveContext.getMasterId(),
                slaveContext.getChecksum() );
    }

    public int getCurrentMasterId()
    {
        return this.broker.getMaster().other().getMachineId();
    }
    
    @Override
    public int getMyId()
    {
        return broker.getMyMachineId();
    }
}
