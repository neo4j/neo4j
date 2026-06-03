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
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.fleetmanagement.configuration.Configuration;
import org.neo4j.fleetmanagement.configuration.State;
import org.neo4j.fleetmanagement.transactions.ITransactor;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.logging.Log;

class BaseServiceTest {
    private BaseService baseService;
    private State state;

    private ITransactor mockTransactor;

    @BeforeEach
    void setup() {
        Logger.initLogger(mock(Log.class));

        mockTransactor = mock(ITransactor.class);
        Configuration mockConfiguration = mock(Configuration.class);
        state = new State();
        baseService = Mockito.spy(new BaseService(mockTransactor, state, mockConfiguration));
    }

    @Test
    void handleErrorResponseWhenResponseBodyIsInvalid() {
        var errMsgPrefix = "Failed";
        var responseCode = 500;

        // when response body is empty
        byte[] emptyResponseBody = null;
        baseService.handleErrorResponse(errMsgPrefix, responseCode, emptyResponseBody);
        assertThat(baseService.state.isConnected()).isFalse();

        // when fail to deserialize an error message
        var unparsableResponseBody = "abc".getBytes();
        baseService.handleErrorResponse(errMsgPrefix, responseCode, unparsableResponseBody);
        assertThat(baseService.state.isConnected()).isFalse();

        // when parsed response is invalid
        var parsedInvalidResponseBody = "{}".getBytes();
        baseService.handleErrorResponse(errMsgPrefix, responseCode, parsedInvalidResponseBody);
        assertThat(baseService.state.isConnected()).isFalse();

        // when parsed response has invalid values
        var parsedInvalidValueResponseBody = "{\"code\": 9999}".getBytes();
        baseService.handleErrorResponse(errMsgPrefix, responseCode, parsedInvalidValueResponseBody);
        assertThat(baseService.state.isConnected()).isFalse();
    }

    @Test
    void handleErrorResponseWhenResponseBodyIsValid() {
        var errMsgPrefix = "Failed";

        // token rotation due
        var tokenRotationErr = "{\"code\": 1000, \"message\": \"Token needs rotation\"}";
        baseService.handleErrorResponse(errMsgPrefix, 401, tokenRotationErr.getBytes());
        Mockito.verify(mockTransactor).rotateToken();
        assertThat(state.isRotatingToken()).isTrue();

        // token expired
        var tokenExpiredErr = "{\"code\": 1001, \"message\": \"Token expired\"}";
        baseService.handleErrorResponse(errMsgPrefix, 401, tokenExpiredErr.getBytes());
        Mockito.verify(mockTransactor).deleteToken();
        assertThat(state.isConnected()).isFalse();
        assertThat(state.getConnectionMessage())
                .isEqualTo("Fleet management token is permanently expired - register a new one to resume operation");

        // token revoked
        var tokenRevokedErr = "{\"code\": 1002, \"message\": \"Token revoked\"}";
        baseService.handleErrorResponse(errMsgPrefix, 401, tokenRevokedErr.getBytes());
        Mockito.verify(mockTransactor, Mockito.times(2)).deleteToken();
        assertThat(state.isConnected()).isFalse();
        assertThat(state.getConnectionMessage())
                .isEqualTo("Fleet management token is revoked - register a new one to resume operation");

        // access denied
        var accessDeniedErr = "{\"code\": 1003, \"message\": \"Access denied\"}";
        baseService.handleErrorResponse(errMsgPrefix, 401, accessDeniedErr.getBytes());
        Mockito.verify(mockTransactor, Mockito.times(3)).deleteToken();
        assertThat(state.isConnected()).isFalse();
        assertThat(state.getConnectionMessage()).isEqualTo("Fleet management access denied - check your permissions");
    }
}
