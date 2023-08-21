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
package org.neo4j.bolt.test.annotation.connection.resolver;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.Resolver;
import org.neo4j.bolt.test.connection.resolver.ConnectorAddressResolver;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.configuration.connectors.ConnectorType;

/**
 * Selects the connector to which a connection shall be established via the annotated connection parameter.
 * <p />
 * This annotation is applicable to parameters within {@link BoltTestExtension} methods of types {@link TransportConnection} and
 * {@link ConnectionProvider}.
 */
@Documented
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
@Resolver(ConnectorAddressResolver.class)
public @interface Connector {

    /**
     * Defines the connector to use.
     *
     * @return a connector type.
     */
    ConnectorType value();
}
