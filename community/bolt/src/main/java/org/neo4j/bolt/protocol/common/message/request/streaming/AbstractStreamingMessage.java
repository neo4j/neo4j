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
package org.neo4j.bolt.protocol.common.message.request.streaming;

import java.util.Objects;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;

public abstract sealed class AbstractStreamingMessage implements RequestMessage permits DiscardMessage, PullMessage {

    private final long n;
    private final long statementId;

    public AbstractStreamingMessage(long n, long statementId) {
        this.n = n;
        this.statementId = statementId;
    }

    public long n() {
        return this.n;
    }

    public long statementId() {
        return statementId;
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
        return n == that.n && statementId == that.statementId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(n, statementId);
    }

    @Override
    public String toString() {
        return "n=" + n + ", statementId=" + statementId;
    }
}
