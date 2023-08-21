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
package org.neo4j.bolt.test.annotation.connection.transport;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.neo4j.bolt.test.annotation.connection.SelectTransport;
import org.neo4j.bolt.test.connection.transport.FilteredTransportSelector;
import org.neo4j.bolt.testing.client.TransportType;

/**
 * Selects a list of transports for execution of one or more annotated test templates.
 * <p />
 * This is a meta annotation.
 *
 * @see ExcludeTransport to create a blacklist or filter the transports provided within this list.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.TYPE})
@SelectTransport(FilteredTransportSelector.class)
public @interface IncludeTransport {

    /**
     * Explicitly selects a list of transports to be included from test execution.
     *
     * @return a list of included transport types.
     */
    TransportType[] value();
}
