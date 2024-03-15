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
package org.neo4j.token.api;

import java.util.Objects;
import org.neo4j.string.Mask;

/**
 * A token with its associated name.
 */
public final class NamedToken {
    private final int id;
    private final String name;
    private final boolean internal;

    public NamedToken(String name, int id) {
        this(name, id, false);
    }

    public NamedToken(String name, int id, boolean internal) {
        this.id = id;
        this.name = name;
        this.internal = internal;
    }

    /**
     * Id of token
     *
     * @return the id of the token
     */
    public int id() {
        return id;
    }

    /**
     * The name associated with the token
     *
     * @return The name corresponding to the token
     */
    public String name() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NamedToken that = (NamedToken) o;

        return id == that.id && internal == that.internal && name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, internal);
    }

    @Override
    public String toString() {
        return toString(Mask.NO);
    }

    public String toString(Mask mask) {
        return String.format(
                "%s[name:%s, id:%d, internal:%s]", getClass().getSimpleName(), mask.filter(name), id, internal);
    }

    public boolean isInternal() {
        return internal;
    }
}
