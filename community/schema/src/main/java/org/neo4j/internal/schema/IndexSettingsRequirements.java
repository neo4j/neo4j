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
package org.neo4j.internal.schema;

import static org.neo4j.values.utils.ValueTypeNames.nameOfType;

import java.util.Objects;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.schema.IndexConfigUtils.IndexSettingsRequirement;

public class IndexSettingsRequirements {
    private IndexSettingsRequirements() {}

    public static class DefaultRequirement<T> implements IndexSettingsRequirement<T> {
        protected final T requirement;

        public DefaultRequirement(T requirement) {
            this.requirement = requirement;
        }

        @Override
        public T get() {
            return requirement;
        }

        @Override
        public String supported() {
            return Objects.toString(requirement);
        }

        @Override
        public String toString() {
            Class<?> type = getClass();
            String simpleName = type.getSimpleName();
            String typeName = simpleName.isBlank() ? type.getTypeName() : simpleName;
            return "%s[requirement=%s]".formatted(typeName, requirement);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(requirement);
        }

        @Override
        public boolean equals(Object object) {
            if (object == null || this.getClass() != object.getClass()) {
                return false;
            }
            DefaultRequirement<?> that = (DefaultRequirement<?>) object;
            return Objects.equals(this.requirement, that.requirement);
        }
    }

    public static class ClassRequirement extends DefaultRequirement<Class<?>> {
        public ClassRequirement(Class<?> requirement) {
            super(requirement);
        }

        @Override
        public String supported() {
            return "non-null instance of " + nameOfType(requirement);
        }
    }

    public static final class IterableRequirement extends DefaultRequirement<Iterable<?>> {
        public IterableRequirement(Iterable<?> requirement) {
            super(Iterables.asUnmodifiable(requirement));
        }

        @Override
        public String supported() {
            return Iterables.toString(requirement, ", ", "[", "]");
        }
    }
}
