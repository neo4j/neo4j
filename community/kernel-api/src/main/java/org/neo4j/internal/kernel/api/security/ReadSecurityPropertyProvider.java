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
package org.neo4j.internal.kernel.api.security;

import org.eclipse.collections.api.factory.primitive.IntObjectMaps;
import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.storageengine.api.StorageProperty;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.values.storable.Value;

public interface ReadSecurityPropertyProvider {
    IntObjectMap<Value> getSecurityProperties();

    class LazyReadSecurityPropertyProvider implements ReadSecurityPropertyProvider {
        private final StoragePropertyCursor securityPropCursor;
        private MutableIntObjectMap<Value> properties;
        private final Iterable<StorageProperty> txStateChangedProperties;
        private final PropertySelection securityProperties;

        public LazyReadSecurityPropertyProvider(
                StoragePropertyCursor securityPropCursor,
                Iterable<StorageProperty> txStateChangedProperties,
                PropertySelection securityProperties) {
            this.securityPropCursor = securityPropCursor;
            this.txStateChangedProperties = txStateChangedProperties;
            this.securityProperties = securityProperties;
        }

        @Override
        public IntObjectMap<Value> getSecurityProperties() {
            if (properties == null) {
                properties = IntObjectMaps.mutable.empty();
                while (securityPropCursor.next()) {
                    properties.put(securityPropCursor.propertyKey(), securityPropCursor.propertyValue());
                }
                if (txStateChangedProperties != null) {
                    for (StorageProperty changedProperty : txStateChangedProperties) {
                        if (securityProperties != null) {
                            if (securityProperties.test(changedProperty.propertyKeyId())) {
                                properties.put(changedProperty.propertyKeyId(), changedProperty.value());
                            }
                        } else {
                            properties.put(changedProperty.propertyKeyId(), changedProperty.value());
                        }
                    }
                }
            }
            return properties;
        }
    }
}
