/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha.com.master;

import java.util.HashMap;
import java.util.Map;

import javax.transaction.SystemException;
import javax.transaction.Transaction;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.com.RequestContext;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MasterImplTest
{
    @Test
    public void givenStartedAndInaccessibleWhenInitializeTxThenThrowException() throws Throwable
    {
        // Given
        MasterImpl.SPI spi = mock( MasterImpl.SPI.class );
        Logging logging = new DevNullLoggingService();
        Config config = config( 20 );

        when( spi.isAccessible() ).thenReturn( false );

        MasterImpl instance = new MasterImpl( spi, mock( MasterImpl.Monitor.class ), logging, config );
        instance.start();

        // When
        try
        {
            instance.initializeTx( new RequestContext( 0, 1, 2, new RequestContext.Tx[0], 1, 0 ) );
            fail();
        }
        catch ( TransactionFailureException e )
        {
            // Ok
        }
    }

    @Test
    public void givenStartedAndAccessibleWhenInitializeTxThenSucceeds() throws Throwable
    {
        // Given
        MasterImpl.SPI spi = mock( MasterImpl.SPI.class );
        Logging logging = new DevNullLoggingService();
        Config config = config( 20 );

        when( spi.isAccessible() ).thenReturn( true );
        when( spi.beginTx() ).thenReturn( mock( Transaction.class ) );
        when( spi.getMasterIdForCommittedTx( anyLong() ) ).thenReturn( Pair.of( 1, 1L ) );

        MasterImpl instance = new MasterImpl( spi, mock( MasterImpl.Monitor.class ), logging, config );
        instance.start();
        HandshakeResult handshake = instance.handshake( 1, new StoreId() ).response();

        // When
        try
        {
            instance.initializeTx( new RequestContext( handshake.epoch(), 1, 2, new RequestContext.Tx[0], 1, 0 ) );
        }
        catch ( Exception e )
        {
            fail( e.getMessage() );
        }

    }

    @Test
    public void failingToStartTxShouldNotLeadToNPE() throws Throwable
    {
        // Given
        MasterImpl.SPI spi = mock( MasterImpl.SPI.class );
        Config config = config( 20 );

        when( spi.isAccessible() ).thenReturn( true );
        when( spi.beginTx() ).thenThrow( new SystemException("Nope") );
        when( spi.getMasterIdForCommittedTx( anyLong() ) ).thenReturn( Pair.of( 1, 1L ) );

        MasterImpl instance = new MasterImpl( spi, mock( MasterImpl.Monitor.class ),
                new DevNullLoggingService(), config );
        instance.start();
        HandshakeResult handshake = instance.handshake( 1, new StoreId() ).response();

        // When
        try
        {
            instance.initializeTx( new RequestContext( handshake.epoch(), 1, 2, new RequestContext.Tx[0], 1, 0 ) );
            fail("Should have failed.");
        }
        catch ( Exception e )
        {
            // Then
            assertThat(e.getCause(), instanceOf( SystemException.class ));
            assertThat(e.getCause().getMessage(), equalTo( "Nope" ));
        }
    }

    private Config config( int lockReadTimeout )
    {
        Map<String, String> params = new HashMap<String, String>();
        params.put( HaSettings.lock_read_timeout.name(), lockReadTimeout + "s" );
        params.put( ClusterSettings.server_id.name(), "1" );
        return new Config( params, HaSettings.class );
    }
}
