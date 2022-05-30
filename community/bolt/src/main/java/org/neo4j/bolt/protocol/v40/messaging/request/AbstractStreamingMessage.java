/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.bolt.protocol.v40.messaging.request;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.values.virtual.MapValue;

public abstract class AbstractStreamingMessage implements RequestMessage {

    private final MapValue meta;
    private final long n;
    private final int statementId;

    public AbstractStreamingMessage(MapValue meta, long n, int statementId) {
        this.meta = requireNonNull(meta);
        this.n = n;
        this.statementId = statementId;
    }

    public long n() {
        return this.n;
    }

    public int statementId() {
        return statementId;
    }

    public MapValue meta() {
        return meta;
    }

    @Override
    public boolean safeToProcessInAnyState() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractStreamingMessage that = (AbstractStreamingMessage) o;
        return Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(meta);
    }

    @Override
    public String toString() {
        return String.format("%s %s", name(), meta());
    }

    abstract String name();
}
