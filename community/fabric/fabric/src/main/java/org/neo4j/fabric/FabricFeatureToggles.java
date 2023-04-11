/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric;

import org.neo4j.util.FeatureToggles;

public class FabricFeatureToggles {

    public static class Toggle {
        private final Class<?> location = FabricFeatureToggles.class;
        private final String name;
        private final boolean defaultValue;

        Toggle(String name, boolean defaultValue) {
            this.name = name;
            this.defaultValue = defaultValue;
        }

        public boolean get() {
            return FeatureToggles.flag(location, name, defaultValue);
        }

        public void set(Boolean enabled) {
            FeatureToggles.set(location, name, enabled);
        }

        public void clear() {
            FeatureToggles.clear(location, name);
        }
    }

    public static final Toggle NEW_COMPOSITE_STACK = new Toggle("new_composite_stack", false);
}
