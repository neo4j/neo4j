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
package org.neo4j.fleetmanagement.logs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import org.neo4j.fleetmanagement.communication.SecurityLogsService;
import org.neo4j.fleetmanagement.logs.model.SecurityLog;
import org.neo4j.fleetmanagement.utils.Logger;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogMonitor;
import org.neo4j.logging.log4j.Neo4jLogMarker;
import org.neo4j.logging.log4j.Neo4jMapMessage;
import org.neo4j.monitoring.Monitors;

class SecurityLogInterceptorTest {
    Monitors monitors = new Monitors();
    LogMonitor mockLogMonitor;
    SecurityLogInterceptor securityLogInterceptor;
    SecurityLogsService mockSecurityLogsService;

    ObjectMapper objectMapper = new ObjectMapper();

    private static Log mockLog;

    @BeforeAll
    static void beforeAll() {
        mockLog = Mockito.mock(Log.class);
        Logger.initLogger(mockLog);
    }

    @BeforeEach
    void setUp() {
        mockLogMonitor = monitors.newMonitor(LogMonitor.class);
        mockSecurityLogsService = Mockito.mock(SecurityLogsService.class);

        securityLogInterceptor = new SecurityLogInterceptor(mockSecurityLogsService);
        monitors.addMonitorListener(securityLogInterceptor);
    }

    @AfterEach
    void tearDown() {
        monitors.removeMonitorListener(securityLogInterceptor);
    }

    @ParameterizedTest
    @EnumSource(
            value = Level.class,
            mode = EnumSource.Mode.EXCLUDE,
            names = {"NONE", "DEBUG"})
    void shouldInterceptSecurityLogs(Level level) throws JsonProcessingException {
        var neo4jMessage = new TestSecurityLogMessage(10);
        neo4jMessage.put("message", "message");
        neo4jMessage.put("level", level.toString());
        neo4jMessage.put("source", "client");
        neo4jMessage.put("type", "security");
        Throwable throwable = null;

        mockLogMonitor.onLogMessage(level, neo4jMessage, throwable);

        assertThat(neo4jMessage.get("message")).isEqualTo("message");

        var interceptedLog = objectMapper.readValue(neo4jMessage.asString("JSON"), SecurityLog.class);
        assertThat(interceptedLog).isNotNull();

        Mockito.verify(mockSecurityLogsService, Mockito.times(1)).add(any(SecurityLog.class));
    }

    static class TestSecurityLogMessage extends Neo4jMapMessage {
        public TestSecurityLogMessage(int initialCapacity) {
            super(initialCapacity);
        }

        @Override
        protected void formatAsString(StringBuilder sb) {
            // not needed
        }

        @Override
        public Neo4jLogMarker getMarker() {
            return super.getMarker();
        }
    }
}
