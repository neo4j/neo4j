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
        assertEquals( (Integer) (value1*value2), response.response() );
        assertTrue( serverImplementation.gotCalled() );
        assertTrue( server.responseHasBeenWritten() );
        client.shutdown();
        server.shutdown();
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
    public void communicateBetweenJvms() throws Exception
    {
        ServerInterface server = new MadeUpServerProcess().start(
                new Long[] {storeIdToUse.getCreationTime(), storeIdToUse.getRandomId()} );
        server.awaitStarted();
        MadeUpClient client = new MadeUpClient( MadeUpServerProcess.PORT, storeIdToUse );
        
        assertEquals( (Integer)(9*5), client.multiply( 9, 5 ).response() );
        client.streamSomeData( new ToAssertionWriter(), 1024*1024*50 /*50 Mb*/ );
        
        client.shutdown();
        server.shutdown();
    }
}
