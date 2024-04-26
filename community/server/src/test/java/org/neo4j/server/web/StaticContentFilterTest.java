/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.web;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.servlet.FilterChain;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;
import org.neo4j.server.configuration.ServerSettings;

class StaticContentFilterTest {

    @Test
    void shouldAddStaticContentHeadersToHtmlResponses() throws Exception {
        // given
        HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getServletPath()).thenReturn("any-file.txt");
        HttpServletResponse response = mock(HttpServletResponse.class);
        FilterChain filterChain = mock(FilterChain.class);

        // when
        new StaticContentFilter(ServerSettings.http_static_content_security_policy.defaultValue())
                .doFilter(request, response, filterChain);

        // then
        verify(response).addHeader("Cache-Control", "no-store");
        verify(response).addHeader("Pragma", "no-cache");
        verify(response)
                .addHeader(
                        "Content-Security-Policy", ServerSettings.http_static_content_security_policy.defaultValue());
        verify(response).addHeader("X-Frame-Options", "DENY");
        verify(response).addHeader("X-Content-Type-Options", "nosniff");
        verify(response).addHeader("X-XSS-Protection", "1; mode=block");
        verify(filterChain).doFilter(request, response);
    }
}
