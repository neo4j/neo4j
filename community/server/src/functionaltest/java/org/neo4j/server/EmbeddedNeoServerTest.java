/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server;

import static org.junit.Assert.assertEquals;
import static org.neo4j.server.WebTestUtils.CLIENT;

import java.io.IOException;

import javax.ws.rs.core.MediaType;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.jmx.Primitives;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.server.rest.FunctionalTestHelper;

import com.sun.jersey.api.client.ClientResponse;

public class EmbeddedNeoServerTest
{
    AbstractGraphDatabase myDb;
    FunctionalTestHelper helper;
    
    @Before
    public void setup() throws IOException {
        myDb = new ImpermanentGraphDatabase();
    }
    
    @After 
    public void teardown() {
        myDb.shutdown();
    }
    
    @Test
    public void shouldBeAbleToStartEmbeddedNeoServer() {
        
        EmbeddedNeoServer srv = new EmbeddedNeoServer(myDb);
        
        srv.start();
        helper = new FunctionalTestHelper(srv.getServer());
        
        ClientResponse response = CLIENT.resource(helper.dataUri()).type(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON).get(ClientResponse.class);
        assertEquals(200,response.getStatus());
        
        srv.stop();
    }
    
    @Test
    public void embeddedServerAPIShouldModifyInjectedDatabase() {
        
        EmbeddedNeoServer srv = new EmbeddedNeoServer(myDb);
        
        srv.start();
        
        long originalNodeNumber = myDb.getManagementBean( Primitives.class ).getNumberOfNodeIdsInUse();
        
        helper = new FunctionalTestHelper(srv.getServer());
        String nodeData = "{\"age\":12}";
        
        ClientResponse response = CLIENT.resource(helper.dataUri()+"node").type(MediaType.APPLICATION_JSON)
                                        .accept(MediaType.APPLICATION_JSON)
                                        .entity( nodeData )
                                        .post(ClientResponse.class);
        assertEquals(201,response.getStatus());
        
        long newNodeNumber = myDb.getManagementBean( Primitives.class ).getNumberOfNodeIdsInUse();
        
        assertEquals(originalNodeNumber + 1, newNodeNumber);
        
        srv.stop();
    }
    
}
