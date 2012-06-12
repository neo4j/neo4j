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

import java.util.Date;

import org.neo4j.server.rest.JaxRsResponse;
import org.neo4j.server.rest.RestRequest;

public class Neo4jServerAPI {

    private String url;

    public Neo4jServerAPI(String url)
    {
        this.url = url;
    }

    public void waitUntilNodeDoesNotExist(long nodeId)
    {
        JaxRsResponse r = null;
        long start = new Date().getTime(), timeout = 1000 * 60 * 1;
        try
        {
            do
            {
                r = RestRequest.req().get(url + "/db/data/node/" + nodeId);
                Thread.sleep(100);
                if (new Date().getTime() - start > timeout)
                {
                    System.out.println(r.getStatus());
                    throw new RuntimeException(
                            "Waiting for node to disappear took longer than the timout specified.");
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
        long start = new Date().getTime(), timeout = 1000 * 60;
        try
        {
            do
            {
                r = RestRequest.req().get(url + "/db/data/node/" + nodeId);
                Thread.sleep(100);
                if (new Date().getTime() - start > timeout)
                {
                    throw new RuntimeException(
                            "Waiting for node to exist took longer than the timout specified.");
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
        return Long.valueOf(parts[parts.length - 1]);
    }

}
