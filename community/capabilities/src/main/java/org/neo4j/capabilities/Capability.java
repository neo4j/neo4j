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
package org.neo4j.capabilities;

import java.util.Objects;

/**
 * This interface provides a declarative way to define capabilities supported.
 *
 * @param <T> The type of the capability value.
 */
public final class Capability<T> {
    private final Name name;
    private final Type<T> type;
    private boolean internal;
    private String description;

    public Capability(Name name, Type<T> type) {
        this(name, type, "", true);
    }

    private Capability(Name name, Type<T> type, String description, boolean internal) {
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        this.description = Objects.requireNonNull(description);
        this.internal = internal;
    }

    /**
     * Name of the capability, defined as a hierarchy - each level separated by `.` (dot).
     *
     * @return the name.
     */
    public Name name() {
        return name;
    }

    /**
     * Description of the capability.
     *
     * @return the description.
     */
    public String description() {
        return description;
    }

    /**
     * Whether this capability is reserved for internal use.
     *
     * @return true if the capability is internal, false otherwise.
     */
    public boolean internal() {
        return internal;
    }

    /**
     * Type of the capability value.
     *
     * @return the type.
     */
    public Type<T> type() {
        return type;
    }

    void setDescription(String description) {
        this.description = Objects.requireNonNull(description);
    }

    void setPublic() {
        this.internal = false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Capability<?> that = (Capability<?>) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return "Capability{" + "name=" + name + ", type=" + type + '}';
    }
}
