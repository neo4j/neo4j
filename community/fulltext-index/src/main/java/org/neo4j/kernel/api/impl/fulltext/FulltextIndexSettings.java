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
package org.neo4j.kernel.api.impl.fulltext;

import static org.neo4j.kernel.api.impl.fulltext.FulltextIndexSettingsKeys.EVENTUALLY_CONSISTENT;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.values.storable.BooleanValue;

final class FulltextIndexSettings {
    private FulltextIndexSettings() {}

    static String[] createPropertyNames(IndexDescriptor descriptor, TokenNameLookup tokenNameLookup) {
        int[] propertyIds = descriptor.schema().getPropertyIds();
        String[] propertyNames = new String[propertyIds.length];
        for (int i = 0; i < propertyIds.length; i++) {
            propertyNames[i] = tokenNameLookup.propertyKeyGetName(propertyIds[i]);
        }
        return propertyNames;
    }

    static boolean isEventuallyConsistent(IndexDescriptor index) {
        BooleanValue eventuallyConsistent =
                index.getIndexConfig().getOrDefault(EVENTUALLY_CONSISTENT, BooleanValue.FALSE);
        return eventuallyConsistent.booleanValue();
    }
}
