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
package org.neo4j.server.queryapi.request;

import java.util.Optional;
import java.util.Set;
import org.neo4j.driver.NotificationClassification;
import org.neo4j.driver.NotificationSeverity;

/**
 * Interface for notifications filter in Query Request.
 * <p/>
 * This object is used to filter notifications returned by the query.
 */
public interface QueryRequestNotificationsFilter {

    /**
     * The minimal severity level for notifications.
     */
    Optional<NotificationSeverity> minimumSeverityLevel();

    /**
     * The set of notification categories that should be disabled.
     */
    Optional<Set<NotificationClassification>> disabledCategories();
}
