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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import org.neo4j.kernel.impl.query.NotificationConfiguration;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;

/**
 * Create a notifications config object that can hold user specified configuration for connection / transaction lifetimes.
 */
public final class SelectiveNotificationsConfig implements NotificationsConfig {
    private final Set<NotificationConfiguration.Category> categoriesToIgnore;
    private final NotificationConfiguration.Severity minimumSeverity;

    /**
     * Create a new user specified notifications config object
     *
     * @param minimumSeverity nullable value to specify minimum severity needed to be emitted by a query.
     * @param categoriesToIgnore nullable value to specify which categories that should be not be checked for.
     * @throws IllegalStructArgumentException when a value specified in {@param minimumSeverity} can not be parsed by {@link NotificationConfiguration.Severity}
     * or when {@param categoriesToIgnore} contains a value that can not be parsed by {@link NotificationConfiguration.Category}
     */
    public SelectiveNotificationsConfig(String minimumSeverity, List<String> categoriesToIgnore)
            throws IllegalStructArgumentException {
        this.categoriesToIgnore = mapCategories(categoriesToIgnore);
        this.minimumSeverity = mapSeverity(minimumSeverity);
    }

    private static NotificationConfiguration.Severity mapSeverity(String minimumSeverity)
            throws IllegalStructArgumentException {
        if (minimumSeverity == null) return null;

        try {
            return Enum.valueOf(NotificationConfiguration.Severity.class, minimumSeverity.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException err) {
            throw new IllegalStructArgumentException("Could not parse a NotificationConfig's minimum severity.", err);
        }
    }

    private static Set<NotificationConfiguration.Category> mapCategories(List<String> cats)
            throws IllegalStructArgumentException {
        if (cats == null) return null;

        var set = new HashSet<NotificationConfiguration.Category>();
        try {
            for (var x : cats) {
                set.add(Enum.valueOf(NotificationConfiguration.Category.class, x.toUpperCase(Locale.ROOT)));
            }
        } catch (IllegalArgumentException err) {
            throw new IllegalStructArgumentException("Could not parse a NotificationConfig category to ignore.", err);
        }

        return set;
    }

    @Override
    public NotificationConfiguration buildConfiguration(NotificationsConfig parentConfig) {
        var sev = minimumSeverity;
        var cats = categoriesToIgnore;

        if (parentConfig instanceof SelectiveNotificationsConfig connectionConfig) {
            if (sev == null) {
                sev = connectionConfig.minimumSeverity;
            }
            if (cats == null) {
                cats = connectionConfig.categoriesToIgnore;
            }
        }

        if (sev == null) {
            sev = NotificationConfiguration.Severity.INFORMATION;
        }

        if (cats == null) {
            cats = Collections.emptySet();
        }

        return new NotificationConfiguration(sev, cats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o instanceof SelectiveNotificationsConfig that) {
            return Objects.equals(this.minimumSeverity, that.minimumSeverity)
                    && Objects.equals(this.categoriesToIgnore, that.categoriesToIgnore);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(minimumSeverity, categoriesToIgnore);
    }

    @Override
    public String toString() {
        return "{ minimumSeverity=" + minimumSeverity.toString() + ", categoriesToIgnore="
                + categoriesToIgnore.toString() + " }";
    }
}
