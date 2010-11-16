/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.rest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.junit.Before;
import org.junit.Test;

public class JmxServiceTest
{
    public JmxService jmxService;

    @Test
    public void correctRepresentation() throws URISyntaxException
    {
        UriInfo mockUri = mock(UriInfo.class);
        URI uri = new URI("http://peteriscool.com:6666/");
        when(mockUri.getBaseUri()).thenReturn(uri);
        Response resp = jmxService.getServiceDefinition( mockUri );

        assertEquals(200, resp.getStatus());
        assertThat((String)resp.getEntity(), containsString("resources"));
        assertThat((String)resp.getEntity(), containsString(uri.toString()));
        assertThat((String)resp.getEntity(), containsString("jmx/domain/{domain}/{objectName}"));
    }

    @Before
    public void setUp() throws Exception
    {
        this.jmxService = new JmxService(  );
    }

}
