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
import org.neo4j.values.virtual.MapValue;

public final class RunMessage extends AbstractTransactionInitiatingMessage {

    public static final byte SIGNATURE = 0x10;

    private final String statement;
    private final MapValue params;

    public RunMessage(String statement) {
        this(statement, MapValue.EMPTY);
    }

    public RunMessage(String statement, MapValue params) {
        super();
        this.statement = statement;
        this.params = params;
    }

    public RunMessage(
            String statement,
            MapValue params,
            List<String> bookmarks,
            Duration txTimeout,
            AccessMode accessMode,
            Map<String, Object> txMetadata,
            String databaseName,
            String impersonatedUser,
            NotificationsConfig notificationsConfig) {
        super(bookmarks, txTimeout, accessMode, txMetadata, databaseName, impersonatedUser, notificationsConfig);
        this.statement = statement;
        this.params = params;
    }

    public String statement() {
        return statement;
    }

    public MapValue params() {
        return params;
    }

    @Override
    public TransactionType type() {
        return TransactionType.IMPLICIT;
    }

    @Override
    public boolean requiresAdmissionControl() {
        return true;
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
        RunMessage that = (RunMessage) o;
        return Objects.equals(statement, that.statement) && Objects.equals(params, that.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), statement, params);
    }

    @Override
    public String toString() {
        return "RunMessage{" + super.toString() + ", " + "statement='" + statement + '\'' + ", params=" + params + '}';
    }
}
