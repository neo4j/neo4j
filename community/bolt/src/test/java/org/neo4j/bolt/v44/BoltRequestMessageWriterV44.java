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
package org.neo4j.bolt.v44;

import static org.neo4j.kernel.impl.util.ValueUtils.asListValue;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.neo4j.bolt.messaging.BoltRequestMessageWriter;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.v43.BoltRequestMessageWriterV43;
import org.neo4j.bolt.v44.messaging.request.RouteMessage;

public class BoltRequestMessageWriterV44 extends BoltRequestMessageWriterV43 {

    public BoltRequestMessageWriterV44(Neo4jPack.Packer packer) {
        super(packer);
    }

    @Override
    public BoltRequestMessageWriter write(RequestMessage message) throws IOException {
        if (message instanceof RouteMessage) {
            this.writeRouteMessage((RouteMessage) message);
            return this;
        }

        return super.write(message);
    }

    private void writeRouteMessage(RouteMessage message) {
        try {
            packer.packStructHeader(0, RouteMessage.SIGNATURE);
            packer.pack(message.getRequestContext());
            packer.pack(asListValue(
                    message.getBookmarks().stream().map(Object::toString).collect(Collectors.toList())));

            var extra = new HashMap<String, Object>();
            if (message.getDatabaseName() != null) {
                extra.put("db", message.getDatabaseName());
            }
            if (message.impersonatedUser() != null) {
                extra.put("imp_user", message.impersonatedUser());
            }

            packer.pack(asMapValue(extra));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
