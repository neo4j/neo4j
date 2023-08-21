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
package org.neo4j.bolt.protocol.v40.message.decoder.authentication;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.AbstractLegacyHelloMessageDecoder;
import org.neo4j.bolt.protocol.common.message.decoder.authentication.DefaultHelloMessageDecoder;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.protocol.common.message.request.connection.RoutingContext;
import org.neo4j.packstream.error.reader.PackstreamReaderException;

public final class HelloMessageDecoderV40 extends AbstractLegacyHelloMessageDecoder {
    private static final HelloMessageDecoderV40 INSTANCE = new HelloMessageDecoderV40();

    private static final List<String> FIELDS = List.of(
            DefaultHelloMessageDecoder.FIELD_FEATURES,
            DefaultHelloMessageDecoder.FIELD_USER_AGENT,
            DefaultHelloMessageDecoder.FILED_NOTIFICATIONS_MIN_SEVERITY,
            DefaultHelloMessageDecoder.FILED_NOTIFICATIONS_DISABLED_CATEGORIES);

    private HelloMessageDecoderV40() {}

    public static HelloMessageDecoderV40 getInstance() {
        return INSTANCE;
    }

    @Override
    protected List<String> providedFields() {
        return FIELDS;
    }

    @Override
    protected RoutingContext readRoutingContext(Map<String, Object> meta) throws PackstreamReaderException {
        return new RoutingContext(false, Collections.emptyMap());
    }

    @Override
    protected NotificationsConfig readNotificationsConfig(Map<String, Object> meta) {
        return null;
    }
}
