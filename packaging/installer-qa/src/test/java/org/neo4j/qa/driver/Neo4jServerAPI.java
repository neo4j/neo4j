/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.qa.driver;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;

public class Neo4jServerAPI {

    private String url;
    private final long timeout;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public Neo4jServerAPI(String url)
    {
        this.url = url;
        timeout = 1000 * 120 * 1;
    }

    public void waitUntilNodeDoesNotExist(long nodeId)
    {
        JaxRsResponse r = null;
        long start = new Date().getTime();
        try
        {
            do
            {
                r = RestRequest.req().get(url + "/db/data/node/" + nodeId);
                Thread.sleep(100);
                if (new Date().getTime() - start > timeout)
                {
                    throw new RuntimeException(
                            "Waiting for node to disappear took longer than than "+timeout+"ms.");
                }
            } while (r.getStatus() == 200);
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void deleteNode(long nodeId)
    {
        JaxRsResponse r = RestRequest.req().delete(
                url + "/db/data/node/" + nodeId);
        if (r.getStatus() != 204)
        {
            throw new RuntimeException("Unable to delete node " + nodeId
                    + ". HTTP status was: " + r.getStatus());
        }
    }

    public void waitUntilNodeExists(long nodeId)
    {
        JaxRsResponse r = null;
        long start = new Date().getTime();
        try
        {
            do
            {
                String fullUrl = url + "/db/data/node/" + nodeId;
                r = RestRequest.req().get(fullUrl);
                Thread.sleep(100);
                if (new Date().getTime() - start > timeout)
                {
                    throw new RuntimeException(
                            "Waiting for node to exist took longer than "+timeout+"ms.");
                }
            } while (r.getStatus() != 200);
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public long createNode()
    {
        JaxRsResponse r = RestRequest.req().post(url + "/db/data/node", "{}");
        if (r.getStatus() != 201)
        {
            throw new RuntimeException(
                    "Unable to create node. HTTP status was: " + r.getStatus());
        }
        String[] parts = r.getLocation().getPath().split("/");
        Long nodeId = Long.valueOf(parts[parts.length - 1]);
        return nodeId;
    }

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> getNeo4jJmxBeans() {
		JaxRsResponse r = RestRequest.req().get(url + "/db/manage/server/jmx/domain/org.neo4j/");
        if (r.getStatus() != 200)
        {
            throw new RuntimeException(
                    "Unable to fetch jmx data. HTTP status was: " + r.getStatus());
        }
        
		try {
			Map<String, Object> json = OBJECT_MAPPER.readValue( r.getEntity(), Map.class );
			return (List<Map<String, Object>>) json.get("beans");
		} catch (JsonMappingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}
