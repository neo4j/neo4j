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
package org.neo4j.qa.machinestate.verifier;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.neo4j.qa.driver.Neo4jDriver;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;

import com.sun.jersey.api.client.ClientHandlerException;

public class Neo4jRestAPIState implements Verifier {

    private enum AssertState {
        RESPONDS,
        DOESNT_RESPOND
    }

    private AssertState assertState;
    
    public Neo4jRestAPIState(AssertState assertState)
    {
        this.assertState = assertState;
    }

    public static Neo4jRestAPIState neo4jRestAPIResponds() {
        return new Neo4jRestAPIState(AssertState.RESPONDS);
    }
    
    public static Neo4jRestAPIState neo4jRestAPIDoesNotRespond() {
        return new Neo4jRestAPIState(AssertState.DOESNT_RESPOND);
    }

    @Override
    public void verify(Neo4jDriver driver)
    {
        switch(assertState) {
        case RESPONDS:
            assertRESTWorks(driver);
            break;
        case DOESNT_RESPOND:
            assertRESTDoesNotWork(driver);
            break;
        }
        
    }
    
    private void assertRESTDoesNotWork(Neo4jDriver driver)
    {
        try
        {
            assertRESTWorks(driver);
            fail("Server is still listening to port 7474, was expecting server to be turned off.");
        } catch (ClientHandlerException e)
        {
            // no-op
        }
    }

    private void assertRESTWorks(Neo4jDriver driver)
    {
        String url = "http://"+driver.vm().definition().ip()+":7474/db/data/";
        JaxRsResponse r = RestRequest.req().get(url);
        assertThat(url + " responds with HTTP 200 on a HTTP GET request.", r.getStatus(), equalTo(200));
    }
    
}
