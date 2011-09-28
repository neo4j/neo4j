/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.com;

import static java.lang.System.currentTimeMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.StoreId;

public class TestCommunication
{
    private static final int PORT = 1234;
    private static final String PATH = "target/tmp";
    private StoreId storeIdToUse;
    
    @Before
    public void doBefore()
    {
        storeIdToUse = new StoreId();
        new File( PATH ).mkdirs();
    }
    
    @Test
    public void clientGetResponseFromServerViaComLayer() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = new MadeUpServer( serverImplementation, PORT );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse );
        
        int value1 = 10;
        int value2 = 5;
        Response<Integer> response = client.multiply( 10, 5 );
        waitUntilResponseHasBeenWritten( server, 1000 );
        assertEquals( (Integer) (value1*value2), response.response() );
        assertTrue( serverImplementation.gotCalled() );
        assertTrue( server.responseHasBeenWritten() );
        client.shutdown();
        server.shutdown();
    }
    
    private void waitUntilResponseHasBeenWritten( MadeUpServer server, int maxTime ) throws Exception
    {
        long time = currentTimeMillis();
        while ( !server.responseHasBeenWritten() && currentTimeMillis()-time < maxTime )
        {
            Thread.sleep( 50 );
        }
    }

    @Test
    public void makeSureClientStoreIdsMustMatch() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = new MadeUpServer( serverImplementation, PORT );
        MadeUpClient client = new MadeUpClient( PORT, new StoreId( 10, 10 ) );
        
        try
        {
            client.multiply( 1, 2 );
            fail();
        }
        catch ( ComException e )
        {
            // Good
        }
        finally
        {
            client.shutdown();
            server.shutdown();
        }
    }

    @Test
    public void makeSureServerStoreIdsMustMatch() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( new StoreId( 10, 10 ) );
        MadeUpServer server = new MadeUpServer( serverImplementation, PORT );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse );
        
        try
        {
            client.multiply( 1, 2 );
            fail();
        }
        catch ( ComException e )
        {
            // Good
        }
        finally
        {
            client.shutdown();
            server.shutdown();
        }
    }
    
    @Test
    public void makeSureClientCanStreamBigData() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = new MadeUpServer( serverImplementation, PORT );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse );
        
        client.streamSomeData( new ToAssertionWriter(), 1024*1024*50 /*50 Mb*/ );
        
        client.shutdown();
        server.shutdown();
    }
    
    @Test
    public void clientThrowsServerSideErrorMidwayThroughStreaming() throws Exception
    {
        final String failureMessage = "Just failing";
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse )
        {
            @Override
            public Response<Void> streamSomeData( MadeUpWriter writer, int dataSize )
            {
                writer.write( new FailingByteChannel( dataSize, failureMessage ) );
                return new Response<Void>( null, storeIdToUse, TransactionStream.EMPTY );
            }
        };
        MadeUpServer server = new MadeUpServer( serverImplementation, PORT );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse );
        
        try
        {
            client.streamSomeData( new ToAssertionWriter(), 1024*1024*20 /*20 Mb, the important thing here is that it must be bigger than one chunk*/ );
            fail( "Should have thrown " + MadeUpException.class.getSimpleName() );
        }
        catch ( ComException e )
        {
            assertCause( e, MadeUpException.class, failureMessage );
        }
        
        client.shutdown();
        server.shutdown();
    }
    
    @Test
    public void communicateBetweenJvms() throws Exception
    {
        ServerInterface server = new MadeUpServerProcess().start(
                new Long[] {storeIdToUse.getCreationTime(), storeIdToUse.getRandomId()} );
        server.awaitStarted();
        MadeUpClient client = new MadeUpClient( MadeUpServerProcess.PORT, storeIdToUse );
        
        assertEquals( (Integer)(9*5), client.multiply( 9, 5 ).response() );
        client.streamSomeData( new ToAssertionWriter(), 1024*1024*10 );
        
        client.shutdown();
        server.shutdown();
    }
    
    @Test
    public void throwingServerSideExceptionBackToClient() throws Exception
    {
        MadeUpImplementation serverImplementation = new MadeUpImplementation( storeIdToUse );
        MadeUpServer server = new MadeUpServer( serverImplementation, PORT );
        MadeUpClient client = new MadeUpClient( PORT, storeIdToUse );
        
        String exceptionMessage = "The message";
        try
        {
            client.throwException( exceptionMessage );
            fail( "Should have thrown " + MadeUpException.class.getSimpleName() );
        }
        catch ( ComException e )
        {
            assertCause( e, MadeUpException.class, exceptionMessage );
        }
        
        client.shutdown();
        server.shutdown();
    }

    private <E extends Exception> void assertCause( ComException comException,
            Class<E> expectedCause, String expectedCauseMessagee )
    {
        Throwable cause = comException.getCause();
        assertTrue( expectedCause.isInstance( cause ) );
        assertEquals( expectedCauseMessagee, cause.getMessage() );
    }
}
