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
package org.neo4j.server.security.ssl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jetty.http.HttpHeader.STRICT_TRANSPORT_SECURITY;
import static org.eclipse.jetty.server.HttpConfiguration.Customizer;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.util.ListIterator;
import org.eclipse.jetty.http.HttpField;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.http.PreEncodedHttpField;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;

class HttpsRequestCustomizerTest {
    @Test
    void shouldSetRequestSchemeToHttps() {
        Customizer customizer = newCustomizer();
        Request request = newRequest();

        var customizedReq = customize(customizer, request, null);

        assertThat(customizedReq.getHttpURI()).isEqualTo(HttpURI.build("https://example.com"));
    }

    @Test
    void shouldAddHstsHeaderWhenConfigured() {
        String configuredValue = "max-age=3600; includeSubDomains";
        Customizer customizer = newCustomizer(configuredValue);
        Request request = newRequest();
        ListIterator<HttpField> responseHeaders = mock(ListIterator.class);

        customize(customizer, request, responseHeaders);

        var stsHeader = new PreEncodedHttpField(STRICT_TRANSPORT_SECURITY, configuredValue);

        verify(responseHeaders).add(eq(stsHeader));
    }

    @Test
    void shouldNotAddHstsHeaderWhenNotConfigured() {
        Customizer customizer = newCustomizer();
        Request request = newRequest();
        ListIterator<HttpField> responseHeaders = mock(ListIterator.class);

        customize(customizer, request, responseHeaders);

        verifyNoInteractions(responseHeaders);
    }

    private static Request customize(Customizer customizer, Request request, ListIterator<HttpField> responseHeaders) {
        var response = mock(Response.class);
        when(response.getHeaders()).thenReturn(index -> responseHeaders);
        return customizer.customize(request, response.getHeaders());
    }

    private static Request newRequest() {
        HttpChannel channel = mock(HttpChannel.class);
        Request request = mock(Request.class);
        var httpUri = HttpURI.build("http://example.com");
        when(request.getHttpURI()).thenReturn(httpUri);
        when(channel.getRequest()).thenReturn(request);
        return request;
    }

    private static Customizer newCustomizer() {
        return new HttpsRequestCustomizer(Config.defaults());
    }

    private static Customizer newCustomizer(String hstsValue) {
        Config config = Config.defaults(ServerSettings.http_strict_transport_security, hstsValue);
        return new HttpsRequestCustomizer(config);
    }
}
