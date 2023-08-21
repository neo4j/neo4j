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
package org.neo4j.bolt.protocol.common.message.request.transaction;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.neo4j.bolt.protocol.common.message.AccessMode;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.tx.TransactionType;

public final class BeginMessage extends AbstractTransactionInitiatingMessage {

    public static final byte SIGNATURE = 0x11;

    private final TransactionType type;

    public BeginMessage(
            List<String> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser,
            TransactionType type,
            NotificationsConfig notificationsConfig) {
        super(bookmarks, txTimeout, accessMode, txMetadata, databaseName, impersonatedUser, notificationsConfig);

        if (type != null) {
            this.type = type;
        } else {
            this.type = TransactionType.EXPLICIT;
        }
    }

    public BeginMessage(
            List<String> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser) {
        this(bookmarks, txTimeout, accessMode, txMetadata, databaseName, impersonatedUser, null, null);
    }

    public BeginMessage(
            List<String> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName) {
        this(bookmarks, txTimeout, accessMode, txMetadata, databaseName, null);
    }

    @Override
    public TransactionType type() {
        return type;
    }

    @Override
    @SuppressWarnings("removal")
    public boolean isIgnoredWhenFailed() {
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
        if (!super.equals(o)) {
            return false;
        }
        BeginMessage that = (BeginMessage) o;
        return type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), type);
    }

    @Override
    public String toString() {
        return "BeginMessage{" + super.toString() + ", " + "type=" + type + '}';
    }
}
