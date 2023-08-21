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
package org.neo4j.bolt.testing.sequence;

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.List;
import org.neo4j.bolt.testing.client.TransportConnection;

public class RequestSequence {
    private final List<ByteBuf> requests;

    public RequestSequence(ByteBuf... requests) {
        this.requests = List.of(requests);
    }

    public List<ByteBuf> requests() {
        return this.requests;
    }

    public int requestCount() {
        return this.requests.size();
    }

    public void execute(TransportConnection connection) throws IOException {
        for (var request : this.requests) {
            connection.send(request);
        }
    }

    public void assertSuccess(TransportConnection connection) {
        assertThat(connection).receivesSuccess(this.requests.size());
    }

    public void assertFailure(TransportConnection connection) {
        assertThat(connection).receivesFailure(this.requests.size());
    }

    public void assertResponse(TransportConnection connection) {
        assertThat(connection).receivesResponse(this.requests.size());
    }

    public void assertResponseOrRecord(TransportConnection connection) {
        assertThat(connection).receivesResponseOrRecord(this.requests.size());
    }
}
