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
package org.neo4j.bolt.test.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.extension.BoltTestSupportExtension;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

/**
 * Marks the annotated function as a Bolt test which is to be executed against a predefined set of transports and
 * protocol versions.
 * <p />
 * This annotation is equivalent to {@link TestTemplate} and will automatically bootstrap a server instance for each
 * respective configuration combination within the test matrix.
 * <p />
 * Test functions may additionally rely on injected parameters for {@link TransportConnection},
 * {@link TransportConnection.Factory} and {@link BoltWire} in order to facilitate transport and protocol agnostic
 * testing.
 * <p />
 * This a meta-annotation.
 */
@Documented
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(BoltTestSupportExtension.class)
public @interface BoltTestExtension {

    /**
     * Selects a test database management service builder which shall be used in order to instantiate a new server
     * instance for testing.
     *
     * @return a database management service builder.
     */
    Class<? extends TestDatabaseManagementServiceBuilder> databaseManagementServiceBuilder() default
            PlaceholderTestDatabaseManagementServiceBuilder.class;

    /**
     * Provides a placeholder value to be used as a default for {@link #databaseManagementServiceBuilder()}.
     */
    final class PlaceholderTestDatabaseManagementServiceBuilder extends TestDatabaseManagementServiceBuilder {
        private PlaceholderTestDatabaseManagementServiceBuilder() {}
    }
}
