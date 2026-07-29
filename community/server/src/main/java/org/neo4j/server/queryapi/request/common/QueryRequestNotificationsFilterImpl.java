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
package org.neo4j.server.queryapi.request.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import org.neo4j.driver.NotificationClassification;
import org.neo4j.driver.NotificationSeverity;
import org.neo4j.server.queryapi.request.QueryRequestNotificationsFilter;

/**
 * Implementation of {@link QueryRequestNotificationsFilter} that allows filtering notifications based on severity level and categories.
 * <p/>
 * This class defines the shape of the filter on the HTTP request body and maps to the internal representation of the filter.
 */
public class QueryRequestNotificationsFilterImpl implements QueryRequestNotificationsFilter {

    private final NotificationSeverity minimumSeverityLevel;
    private final Set<NotificationClassification> disabledCategories;

    private QueryRequestNotificationsFilterImpl(
            NotificationSeverity minimumSeverityLevel, Set<NotificationClassification> disabledCategories) {
        this.minimumSeverityLevel = minimumSeverityLevel;
        this.disabledCategories = disabledCategories;
    }

    @Override
    public Optional<NotificationSeverity> minimumSeverityLevel() {
        return Optional.ofNullable(minimumSeverityLevel);
    }

    @Override
    public Optional<Set<NotificationClassification>> disabledCategories() {
        return Optional.ofNullable(disabledCategories);
    }

    @JsonCreator
    public static QueryRequestNotificationsFilter deserialize(
            @JsonProperty("minimumSeverityLevel") String minimumSeverityLevelName,
            @JsonProperty("disabledCategories") String[] disabledCategoriesNames)
            throws IOException {
        var minimumSeverityLevel = getMinimumSeverityLevel(minimumSeverityLevelName);
        var disabledCategories = getDisabledCategories(disabledCategoriesNames);

        return new QueryRequestNotificationsFilterImpl(minimumSeverityLevel, disabledCategories);
    }

    private static NotificationSeverity getMinimumSeverityLevel(String minimumSeverityLevel) throws JsonParseException {
        if (minimumSeverityLevel == null) {
            return null;
        }

        return switch (minimumSeverityLevel) {
            case "WARNING" -> NotificationSeverity.WARNING;
            case "INFORMATION" -> NotificationSeverity.INFORMATION;
            case "OFF" -> NotificationSeverity.OFF;
            default -> throw new JsonParseException("Invalid minimum severity level: " + minimumSeverityLevel);
        };
    }

    private static Set<NotificationClassification> getDisabledCategories(String[] disabledCategoriesNames)
            throws JsonParseException {
        if (disabledCategoriesNames == null) {
            return null;
        }

        var disabledCategories = EnumSet.noneOf(NotificationClassification.class);

        for (String classification : disabledCategoriesNames) {
            NotificationClassification disabledCategory = getDisabledCategory(classification);
            disabledCategories.add(disabledCategory);
        }

        return disabledCategories;
    }

    private static NotificationClassification getDisabledCategory(String categoryName) throws JsonParseException {
        if (categoryName == null) {
            throw new JsonParseException("Invalid disabled category: null");
        }
        try {
            return NotificationClassification.valueOf(categoryName);
        } catch (IllegalArgumentException e) {
            throw new JsonParseException(null, "Invalid disabled category: " + categoryName, e);
        }
    }
}
