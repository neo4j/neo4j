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
package org.neo4j.fleetmanagement.communication;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.fleetmanagement.common.EmptyResponseException;
import org.neo4j.fleetmanagement.common.ResponseStatusException;
import org.neo4j.fleetmanagement.communication.model.ReportingResponse;
import org.neo4j.fleetmanagement.communication.upstream.Upstream;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.logging.Log;

class AbstractReportingServiceTest {

    private static final Upstream.Endpoint ENDPOINT = Upstream.Endpoint.REPORTING;

    private static Log mockLog;

    private ITransactor mockTransactor;
    private Upstream mockUpstream;
    private Configuration mockConfiguration;
    private State state;
    private Upstream.UpstreamPostRequest mockPostRequest;
    private AbstractReportingService service;

    /** Minimal serializable message for {@link AbstractReportingService#callApi}. */
    private static final class TestMessage {
        @SuppressWarnings("unused")
        public String id = "test";
    }

    private final TestMessage testMessage = new TestMessage();

    @BeforeAll
    static void beforeAll() {
        mockLog = mock(Log.class);
        Logger.initLogger(mockLog);
    }

    @BeforeEach
    void setUp() throws IOException {
        mockTransactor = mock(ITransactor.class);
        mockUpstream = mock(Upstream.class);
        mockConfiguration = mock(Configuration.class);
        state = new State();
        state.setConnected();
        mockPostRequest = mock(Upstream.UpstreamPostRequest.class);
        when(mockUpstream.postTo(ENDPOINT)).thenReturn(mockPostRequest);

        service = new AbstractReportingService(mockTransactor, mockUpstream, state, mockConfiguration) {
            @Override
            public void report() {
                // not used in these tests
            }
        };
    }

    @AfterEach
    void tearDown() {
        Mockito.reset(mockLog);
    }

    @Test
    void callApi_success_deserializesBody() throws IOException {
        when(mockPostRequest.transmit(any(byte[].class))).thenReturn(200);
        when(mockPostRequest.getResponseBody()).thenReturn("{\"status\":\"ok\"}".getBytes(StandardCharsets.UTF_8));

        ReportingResponse result = service.callApi(testMessage, ENDPOINT, ReportingResponse.class);

        assertThat(result.status).isEqualTo("ok");
        assertThat(state.isConnected()).isTrue();
    }

    @Test
    void callApi_successWithEmptyBody_wrapsEmptyResponseExceptionAndDisconnects() throws IOException {
        when(mockPostRequest.transmit(any(byte[].class))).thenReturn(200);
        when(mockPostRequest.getResponseBody()).thenReturn(new byte[0]);

        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> service.callApi(testMessage, ENDPOINT, ReportingResponse.class))
                .withCauseInstanceOf(EmptyResponseException.class);

        assertThat(state.isConnected()).isFalse();
    }

    @Test
    void callApi_non2xx_throwsResponseStatusExceptionAndDisconnects() throws Exception {
        when(mockPostRequest.transmit(any(byte[].class))).thenReturn(500);
        String errJson = "{\"code\": 5000, \"message\": \"Server error\"}";
        when(mockPostRequest.getResponseBody()).thenReturn(errJson.getBytes(StandardCharsets.UTF_8));

        assertThatExceptionOfType(ResponseStatusException.class)
                .isThrownBy(() -> service.callApi(testMessage, ENDPOINT, ReportingResponse.class));

        assertThat(state.isConnected()).isFalse();
    }

    @Test
    void callApi_jsonParseFailure_wrapsAndDisconnects() throws Exception {
        when(mockPostRequest.transmit(any(byte[].class))).thenReturn(200);
        when(mockPostRequest.getResponseBody()).thenReturn("not-json".getBytes(StandardCharsets.UTF_8));

        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> service.callApi(testMessage, ENDPOINT, ReportingResponse.class))
                .withCauseInstanceOf(JsonProcessingException.class);

        assertThat(state.isConnected()).isFalse();
    }

    @Test
    void callApi_ioExceptionOnTransmit_disconnectsAndWraps() throws Exception {
        when(mockPostRequest.transmit(any(byte[].class))).thenThrow(new IOException("network"));

        assertThatExceptionOfType(RuntimeException.class)
                .isThrownBy(() -> service.callApi(testMessage, ENDPOINT))
                .withCauseInstanceOf(IOException.class);

        assertThat(state.isConnected()).isFalse();
    }
}
