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
package org.neo4j.bolt.protocol.common.handler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Test;
import org.neo4j.server.config.AuthConfigProvider;

public class DiscoveryResponseHandlerTest {
    @Test
    void shouldRespondWithHttpResponse() {
        var authConfigMock = mock(AuthConfigProvider.class);
        when(authConfigMock.getRepresentationAsBytes()).thenReturn(new byte[0]);

        var payload = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "http://blah.com");

        var channel = new EmbeddedChannel(new DiscoveryResponseHandler(authConfigMock));

        channel.writeInbound(payload);

        var discoveryResponse = (FullHttpResponse) channel.readOutbound();

        assertThat(discoveryResponse.status()).isEqualTo(HttpResponseStatus.OK);
        assertThat(discoveryResponse.protocolVersion()).isEqualTo(HttpVersion.HTTP_1_1);
        assertThat(discoveryResponse
                        .headers()
                        .contains(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON, false))
                .isTrue();
        assertThat(discoveryResponse.headers().contains(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*", false))
                .isTrue();
        assertThat(discoveryResponse.headers().contains(HttpHeaderNames.VARY, "Accept", false))
                .isTrue();
        assertThat(discoveryResponse
                        .headers()
                        .contains(
                                HttpHeaderNames.CONTENT_LENGTH,
                                String.valueOf(discoveryResponse.content().readableBytes()),
                                false))
                .isTrue();
        assertThat(discoveryResponse.headers().contains(HttpHeaderNames.DATE)).isTrue();

        assertThat(channel.pipeline().get(DiscoveryResponseHandler.class)).isNull();
    }

    @Test
    void shouldNotRespondWhenWebsocketRequest() {
        var authConfigMock = mock(AuthConfigProvider.class);
        when(authConfigMock.getRepresentationAsBytes()).thenReturn(new byte[0]);

        var payload = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "https://blah.com");
        payload.headers().add(HttpHeaderNames.CONNECTION, HttpHeaderValues.UPGRADE);
        payload.headers().add(HttpHeaderNames.UPGRADE, HttpHeaderValues.WEBSOCKET);

        var channel = new EmbeddedChannel(new DiscoveryResponseHandler(authConfigMock));
        channel.writeInbound(payload);

        var out = channel.readOutbound();

        // the handler produces nothing
        assertThat(out).isNull();

        // and removes itself from the pipeline
        assertThat(channel.pipeline().get(DiscoveryResponseHandler.class)).isNull();
    }
}
