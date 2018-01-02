/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.rest.batch;

import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.neo4j.server.rest.web.InternalJettyServletRequest;
import org.neo4j.server.rest.web.InternalJettyServletResponse;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchOperationsTest {

    private final BatchOperations ops = new BatchOperations(null) {
        @Override
        protected void invoke(String method, String path, String body, Integer id, URI targetUri, InternalJettyServletRequest req, InternalJettyServletResponse res) throws IOException, ServletException {
        }
    };

    @Test
    public void testReplaceLocations() throws Exception {
        Map<Integer,String> map=new HashMap<Integer, String>();
        map.put(100,"bar");
        assertEquals("foo", ops.replaceLocationPlaceholders("foo", map));
        assertEquals("foo bar", ops.replaceLocationPlaceholders("foo {100}", map));
        assertEquals("bar foo bar", ops.replaceLocationPlaceholders("{100} foo {100}", map));
        assertEquals("bar bar foo bar bar", ops.replaceLocationPlaceholders("bar {100} foo {100} bar", map));
    }

    @Test
    public void testSchemeInInternalJettyServletRequestForHttp() throws UnsupportedEncodingException
    {
        // when
        InternalJettyServletRequest req = new InternalJettyServletRequest( "POST", "http://localhost:7473/db/data/node", "{'name':'node1'}", new InternalJettyServletResponse() );

        // then
        assertEquals("http",req.getScheme());
    }

    @Test
    public void testSchemeInInternalJettyServletRequestForHttps() throws UnsupportedEncodingException
    {
        // when
        InternalJettyServletRequest req = new InternalJettyServletRequest( "POST", "https://localhost:7473/db/data/node", "{'name':'node1'}", new InternalJettyServletResponse() );

        // then
        assertEquals("https",req.getScheme());
    }

    @Test
    public void shouldForwardMetadataFromOuterRequest() throws Exception
    {
        // Given
        HttpServletRequest mock = mock( HttpServletRequest.class );

        when(mock.getAuthType()).thenReturn( "auth" );
        when(mock.getRemoteAddr()).thenReturn( "127.0.0.1" );
        when(mock.getRemoteHost()).thenReturn( "localhost" );
        when(mock.getRemotePort()).thenReturn( 1 );
        when(mock.getLocalAddr()).thenReturn( "129.0.0.1" );
        when(mock.getLocalPort()).thenReturn( 2 );

        InternalJettyServletRequest req = new InternalJettyServletRequest( "POST",
                "https://localhost:7473/db/data/node", "", new InternalJettyServletResponse(),
                mock );

        // When & then
        assertEquals( "auth", req.getAuthType());
        assertEquals( "127.0.0.1", req.getRemoteAddr());
        assertEquals( "localhost", req.getRemoteHost());
        assertEquals( 1, req.getRemotePort());
        assertEquals( 2, req.getLocalPort());
        assertEquals( "129.0.0.1", req.getLocalAddr());

    }

    @Test
    public void shouldIgnoreUnknownAndUnparseablePlaceholders() throws Throwable
    {
        // When/then
        assertEquals("foo {00000000010001010001001100111000100101010111001101110111}",
                ops.replaceLocationPlaceholders("foo {00000000010001010001001100111000100101010111001101110111}", Collections.<Integer,String>emptyMap() ));
        assertEquals("foo {2147483648}",
                ops.replaceLocationPlaceholders("foo {2147483648}", Collections.<Integer,String>emptyMap() ));
    }
}
