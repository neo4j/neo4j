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
package org.neo4j.procedure.builtin;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Answers.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.SocketAddress;
import java.time.ZoneId;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.connector.connection.Connection;

class ListConnectionResultTest {
    @Test
    void buildResultOnConnectionWithoutClientAddress() {
        var clientAddress = mock(SocketAddress.class);
        when(clientAddress.toString()).thenReturn(StringUtils.EMPTY);

        var connection = mock(Connection.class, RETURNS_MOCKS);
        when(connection.clientAddress()).thenReturn(clientAddress);

        var result = new ListConnectionResult(connection, ZoneId.systemDefault());
        assertEquals(StringUtils.EMPTY, result.clientAddress);
    }
}
