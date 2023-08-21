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

import java.util.function.Supplier;

/**
 * Provides extension developers to declare capabilities.
 */
public interface CapabilitiesRegistry extends Capabilities {
    /**
     * Sets a capability to the given value.
     *
     * @param capability the capability that's being set.
     * @param value      the value to set.
     * @param <T>        the type of the capability value.
     */
    <T> void set(Capability<T> capability, T value);

    /**
     * Sets a capability as a dynamic value that will be evaluated on each access.
     *
     * @param capability   the capability that's being set.
     * @param dynamicValue the supplier function to be evaluated to retrieve the dynamic value.
     * @param <T>          the type of the capability value.
     */
    <T> void supply(Capability<T> capability, Supplier<T> dynamicValue);
}
