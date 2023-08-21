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

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.util.Preconditions;

public class RequestSequenceCollection {
    private final List<RequestSequence> sequences;
    private Iterator<RequestSequence> it;

    public RequestSequenceCollection() {
        this.sequences = new ArrayList<>();
    }

    public RequestSequenceCollection(List<RequestSequence> sequences) {
        this.sequences = new ArrayList<>(sequences);
    }

    public RequestSequenceCollection(RequestSequence... sequences) {
        this(Arrays.asList(sequences));
    }

    public RequestSequenceCollection with(RequestSequence sequence) {
        this.sequences.add(sequence);
        this.it = null;

        return this;
    }

    public RequestSequenceCollection with(ByteBuf... requests) {
        return this.with(new RequestSequence(requests));
    }

    public boolean hasRemaining() {
        if (this.it == null) {
            return false;
        }

        return this.it.hasNext();
    }

    public int execute(TransportConnection connection) throws IOException {
        var total = 0;

        while (this.hasRemaining()) {
            total += this.executeNext(connection).requestCount();
        }

        return total;
    }

    public RequestSequence execute(TransportConnection connection, Random random) throws IOException {
        Preconditions.checkState(!this.sequences.isEmpty(), "No sequences available");

        var sequence = this.sequences.get(random.nextInt(this.sequences.size()));
        sequence.execute(connection);
        return sequence;
    }

    public RequestSequence executeNext(TransportConnection connection) throws IOException {
        if (this.it == null) {
            this.it = this.sequences.iterator();
        }

        var sequence = this.it.next();
        sequence.execute(connection);
        return sequence;
    }
}
