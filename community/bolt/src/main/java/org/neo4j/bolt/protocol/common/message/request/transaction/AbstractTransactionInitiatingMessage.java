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
import org.neo4j.bolt.protocol.common.message.request.ImpersonationRequestMessage;
import org.neo4j.bolt.tx.TransactionType;

public abstract sealed class AbstractTransactionInitiatingMessage implements ImpersonationRequestMessage
        permits BeginMessage, RunMessage {
    private final List<String> bookmarks;
    private final Duration txTimeout;
    private final AccessMode accessMode;
    private final Map<String, Object> txMetadata;
    private final String databaseName;
    private final String impersonatedUser;
    private final NotificationsConfig notificationsConfig;

    protected AbstractTransactionInitiatingMessage() {
        this(List.of(), null, AccessMode.WRITE, Map.of(), null, null, null);
    }

    protected AbstractTransactionInitiatingMessage(
            List<String> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser,
            NotificationsConfig notificationsConfig) {
        this.bookmarks = bookmarks;
        this.txTimeout = txTimeout;
        this.accessMode = accessMode;
        this.txMetadata = txMetadata;
        this.databaseName = databaseName;
        this.impersonatedUser = impersonatedUser;
        this.notificationsConfig = notificationsConfig;
    }

    public List<String> bookmarks() {
        return bookmarks;
    }

    public Duration transactionTimeout() {
        return txTimeout;
    }

    public AccessMode getAccessMode() {
        return accessMode;
    }

    public Map<String, Object> transactionMetadata() {
        return txMetadata;
    }

    public String databaseName() {
        return databaseName;
    }

    @Override
    public String impersonatedUser() {
        return impersonatedUser;
    }

    public abstract TransactionType type();

    public NotificationsConfig notificationsConfig() {
        return notificationsConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AbstractTransactionInitiatingMessage that = (AbstractTransactionInitiatingMessage) o;
        return Objects.equals(bookmarks, that.bookmarks)
                && Objects.equals(txTimeout, that.txTimeout)
                && accessMode == that.accessMode
                && Objects.equals(txMetadata, that.txMetadata)
                && Objects.equals(databaseName, that.databaseName)
                && Objects.equals(impersonatedUser, that.impersonatedUser)
                && Objects.equals(notificationsConfig, that.notificationsConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                bookmarks, txTimeout, accessMode, txMetadata, databaseName, impersonatedUser, notificationsConfig);
    }

    @Override
    public String toString() {
        var notifications = notificationsConfig != null ? notificationsConfig.toString() : "null";
        return "bookmarks=" + bookmarks + ", txTimeout="
                + txTimeout + ", accessMode="
                + accessMode + ", txMetadata="
                + txMetadata + ", databaseName='"
                + databaseName + '\'' + ", impersonatedUser='"
                + impersonatedUser + '\'' + ", notificationsConfig="
                + notifications;
    }
}
