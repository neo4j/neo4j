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
package org.neo4j.bolt.test.annotation.wire.initializer;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.neo4j.bolt.protocol.common.connector.connection.Feature;
import org.neo4j.bolt.test.annotation.wire.InitializeWire;
import org.neo4j.bolt.test.wire.initializer.FeatureBoltWireInitializer;
import org.neo4j.bolt.testing.messages.BoltWire;

/**
 * Ensures that a given set of optional protocol features is negotiated during the authentication phase of the selected
 * wire implementation.
 * <p />
 * If the selected protocol version implicitly enables a given feature, it will be ignored. If you wish to forcefully
 * negotiate a given feature (e.g. to test whether the server refuses to negotiate implicitly supported features),
 * invoke {@link BoltWire#enable(Feature...)} directly instead.
 * <p />
 * This a meta-annotation.
 */
@Documented
@Repeatable(EnableFeatures.class)
@Retention(RetentionPolicy.RUNTIME)
@InitializeWire(FeatureBoltWireInitializer.class)
@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.TYPE})
public @interface EnableFeature {

    /**
     * Selects a list of features which shall be unable when optionally available in a given protocol version.
     *
     * @return a list of features.
     */
    Feature[] value() default {};
}
