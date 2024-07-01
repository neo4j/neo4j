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
package org.neo4j.configuration.connectors;

public enum ConnectorType {
    BOLT(0, BoltConnector.NAME),
    INTRA_BOLT(1, BoltConnector.INTERNAL_NAME),
    HTTP(2, HttpConnector.NAME),
    HTTPS(3, HttpsConnector.NAME),
    PROMETHEUS(4, "prometheus"),
    RAFT(5, "raft-server"),
    CLUSTER(6, "catchup-server"),
    BACKUP(7, "backup-server");

    private final byte code;
    private final String description;

    ConnectorType(int code, String description) {
        this.code = (byte) code;
        this.description = description;
    }

    public static ConnectorType forCode(byte code) {
        for (ConnectorType value : values()) {
            if (value.code == code) return value;
        }
        throw new IllegalArgumentException("Invalid code for ConnectorType: " + code);
    }

    public String description() {
        return description;
    }

    public byte code() {
        return code;
    }
}
