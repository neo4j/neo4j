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
package org.neo4j.server.rest.web;

import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import org.neo4j.server.logging.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This filter collects the User-Agent of the request and stores it in a Set for UDC to report
 * TODO move to a real UDC-mbean
 */
public class CollectUserAgentFilter implements ContainerRequestFilter
{

    private static final Logger log = Logger.getLogger( CollectUserAgentFilter.class );
    private static final String USER_AGENT = "User-Agent";
    private final static Set<String> userAgents = new HashSet<String>();
    static final int SAMPLE_FREQ = 100;
    // race conditions are not important, record first request
    private int counter=SAMPLE_FREQ;

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        if (counter++ < SAMPLE_FREQ) return request;
        counter = 0;
        try {
            List<String> headers = request.getRequestHeader(USER_AGENT);
            if (headers!=null) {
                for (String header : headers) {
                    if (header==null) continue;
                    userAgents.add(header.replaceAll(" .*",""));
                }
            }
        } catch(Exception e) {
            log.debug( "Error retrieving User-Agent from " + request.getPath(),e );
        }
        return request;
    }

    public static Set<String> getUserAgents() {
        return userAgents;
    }
    public static void reset() {
        userAgents.clear();
    }

}
