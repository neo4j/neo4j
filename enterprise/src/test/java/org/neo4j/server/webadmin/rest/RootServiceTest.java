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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.hamcrest.Matchers.*;

import java.net.URI;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.junit.Test;

public class RootServiceTest {
    @Test
    public void shouldAdvertiseServicesWhenAsked() throws Exception {
        UriInfo uriInfo = mock(UriInfo.class);
        URI uri = new URI("http://example.org:7474/");
        when(uriInfo.getBaseUri()).thenReturn(uri);
        
        RootService svc = new RootService();
        Response serviceDefinition = svc.getServiceDefinition(uriInfo, null);
        
        assertEquals(200, serviceDefinition.getStatus());
        assertThat((String)serviceDefinition.getEntity(), containsString(String.format("\"console\" : \"%sserver/console\"", uri.toString())));
        assertThat((String)serviceDefinition.getEntity(), containsString(String.format("\"jmx\" : \"%sserver/jmx\"", uri.toString())));
    }
}
