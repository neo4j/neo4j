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
package org.neo4j.bolt.test.wire.initializer;

import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.neo4j.bolt.test.annotation.wire.InitializeWire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.testing.util.AnnotationUtil;

/**
 * Prepares a Bolt wire implementation for use with a given test specification.
 * <p />
 * This interface is typically implemented in conjunction with an {@link InitializeWire} annotation or a custom annotation
 * bearing the {@link InitializeWire} annotation.
 * <p />
 * Implementations of this interface are expected to provide a publicly accessible no-args construction via which they
 * are resolved when referenced using the {@link InitializeWire} annotation or one of its children.
 * <p />
 * Refer to the {@link org.neo4j.bolt.test.annotation.wire} package for examples on how to utilize this interface
 * in the most optimal fashion.
 */
public interface BoltWireInitializer {

    /**
     * Initializes the given wire implementation for use with the targeted test specification.
     *
     * @param context an extension context.
     * @param wire a previously selected wire implementation.
     */
    void initialize(ExtensionContext context, BoltWire wire);

    static List<BoltWireInitializer> findInitializer(ExtensionContext context) {
        return AnnotationUtil.selectProviders(context, InitializeWire.class, InitializeWire::value, true);
    }
}
