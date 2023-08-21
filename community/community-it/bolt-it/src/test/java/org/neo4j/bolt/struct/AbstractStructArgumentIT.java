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
package org.neo4j.bolt.struct;

import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;

import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.util.function.Consumer;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltV40Wire;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;

public abstract class AbstractStructArgumentIT {

    protected void testFailureWithUnpackableValue(
            TransportConnection connection, Consumer<PackstreamBuf> packer, String expectedMessage) throws IOException {
        connection.send(createRunWith(packer));

        assertThat(connection).receivesFailure(Status.Request.Invalid, expectedMessage);
    }

    protected ByteBuf createRunWith(Consumer<PackstreamBuf> packer) {
        var buf = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, BoltV40Wire.MESSAGE_TAG_RUN))
                .writeString("RETURN $x") // statement
                .writeMapHeader(1) // parameters
                .writeString("x");

        packer.accept(buf);

        return buf.writeMapHeader(0) // extra
                .getTarget();
    }
}
