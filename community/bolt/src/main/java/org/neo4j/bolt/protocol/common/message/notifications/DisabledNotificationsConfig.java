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
package org.neo4j.bolt.protocol.common.message.notifications;

import org.neo4j.kernel.impl.query.NotificationConfiguration;

/**
 * A configuration object for disabling all notifications for the scope of a transaction or connection.
 */
public final class DisabledNotificationsConfig implements NotificationsConfig {
    private static final NotificationsConfig INSTANCE = new DisabledNotificationsConfig();

    private DisabledNotificationsConfig() {}

    public static NotificationsConfig getInstance() {
        return INSTANCE;
    }

    @Override
    public NotificationConfiguration buildConfiguration(NotificationsConfig parentConfig) {
        return NotificationConfiguration.NONE;
    }

    @Override
    public String toString() {
        return "Disabled";
    }
}
