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
package org.neo4j.bolt.protocol.common.message.decoder.authentication;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.message.decoder.util.AuthenticationMetadataUtils;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;

public abstract class AbstractLegacyHelloMessageDecoder extends DefaultHelloMessageDecoder {

    protected abstract List<String> providedFields();

    @Override
    protected Map<String, Object> readAuthToken(Map<String, Object> meta) {
        return AuthenticationMetadataUtils.extractAuthToken(this.providedFields(), meta);
    }

    @Override
    protected NotificationsConfig readNotificationsConfig(Map<String, Object> meta) {
        return null;
    }

    @Override
    protected Map<String, String> readBoltAgent(Map<String, Object> meta) {
        return Collections.emptyMap();
    }
}
