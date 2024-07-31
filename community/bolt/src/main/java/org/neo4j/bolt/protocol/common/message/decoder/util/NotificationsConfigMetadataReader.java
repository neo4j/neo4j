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
package org.neo4j.bolt.protocol.common.message.decoder.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.common.message.notifications.DefaultNotificationsConfig;
import org.neo4j.bolt.protocol.common.message.notifications.DisabledNotificationsConfig;
import org.neo4j.bolt.protocol.common.message.notifications.NotificationsConfig;
import org.neo4j.bolt.protocol.common.message.notifications.SelectiveNotificationsConfig;
import org.neo4j.packstream.error.struct.IllegalStructArgumentException;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;

public final class NotificationsConfigMetadataReader {
    private NotificationsConfigMetadataReader() {}

    private static final String MINIMUM_SEVERITY_KEY = "notifications_minimum_severity";
    private static final String DISABLED_CATEGORIES_KEY = "notifications_disabled_categories";
    private static final String DISABLED_CLASSIFICATION_KEY = "notifications_disabled_classifications";
    private static final String DISABLE_ALL_NOTIFICATIONS = "OFF";

    public static NotificationsConfig readFromMapValue(MapValue meta) throws IllegalStructArgumentException {
        return readFromMapValue(meta, DISABLED_CLASSIFICATION_KEY);
    }

    public static NotificationsConfig readLegacyFromMapValue(MapValue meta) throws IllegalStructArgumentException {
        return readFromMapValue(meta, DISABLED_CATEGORIES_KEY);
    }

    private static NotificationsConfig readFromMapValue(MapValue meta, String disabledClassificationKey)
            throws IllegalStructArgumentException {
        String minimumSeverity = null;
        ArrayList<String> categoriesToIgnore = null;

        if (meta.containsKey(MINIMUM_SEVERITY_KEY)) {
            if (meta.get(MINIMUM_SEVERITY_KEY) instanceof StringValue stringValue) {
                minimumSeverity = stringValue.stringValue();
            } else {
                throw new IllegalStructArgumentException(MINIMUM_SEVERITY_KEY, "Required to be a String");
            }
        }

        if (meta.containsKey(disabledClassificationKey)) {
            if (meta.get(disabledClassificationKey) instanceof ListValue listValue) {
                categoriesToIgnore = new ArrayList<>();
                for (var x : listValue) {
                    if (x instanceof StringValue stringValue) {
                        categoriesToIgnore.add(stringValue.stringValue());
                    } else {
                        throw new IllegalStructArgumentException(
                                disabledClassificationKey, "Required to be a List::String");
                    }
                }
            } else {
                throw new IllegalStructArgumentException(disabledClassificationKey, "Required to be a List::String");
            }
        }

        return getNotificationsConfig(minimumSeverity, categoriesToIgnore);
    }

    public static NotificationsConfig readFromMap(Map<String, Object> meta) throws IllegalStructArgumentException {
        return readFromMap(meta, DISABLED_CLASSIFICATION_KEY);
    }

    public static NotificationsConfig readLegacyFromMap(Map<String, Object> meta)
            throws IllegalStructArgumentException {
        return readFromMap(meta, DISABLED_CATEGORIES_KEY);
    }

    @SuppressWarnings("rawtypes")
    private static NotificationsConfig readFromMap(Map<String, Object> meta, String disabledClassificationKey)
            throws IllegalStructArgumentException {
        String minimumSeverity = null;
        ArrayList<String> categoriesToIgnore = null;

        if (meta.containsKey(MINIMUM_SEVERITY_KEY)) {
            if (meta.get(MINIMUM_SEVERITY_KEY) instanceof String stringValue) {
                minimumSeverity = stringValue;
            } else {
                throw new IllegalStructArgumentException(MINIMUM_SEVERITY_KEY, "Required to be a String");
            }
        }

        if (meta.containsKey(disabledClassificationKey)) {
            if (meta.get(disabledClassificationKey) instanceof List listValue) {
                categoriesToIgnore = new ArrayList<>();
                for (var x : listValue) {
                    if (x instanceof String stringValue) {
                        categoriesToIgnore.add(stringValue);
                    } else {
                        throw new IllegalStructArgumentException(
                                disabledClassificationKey, "Required to be a List::String");
                    }
                }
            } else {
                throw new IllegalStructArgumentException(disabledClassificationKey, "Required to be a List::String");
            }
        }

        return getNotificationsConfig(minimumSeverity, categoriesToIgnore);
    }

    private static NotificationsConfig getNotificationsConfig(
            String minimumSeverity, ArrayList<String> categoriesToIgnore) throws IllegalStructArgumentException {

        if (minimumSeverity == null && categoriesToIgnore == null) {
            return DefaultNotificationsConfig.getInstance();
        }

        if (minimumSeverity != null && minimumSeverity.equalsIgnoreCase(DISABLE_ALL_NOTIFICATIONS)) {
            return DisabledNotificationsConfig.getInstance();
        }

        return new SelectiveNotificationsConfig(minimumSeverity, categoriesToIgnore);
    }
}
