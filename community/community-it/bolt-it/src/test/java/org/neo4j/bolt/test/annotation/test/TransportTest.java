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
package org.neo4j.bolt.test.annotation.test;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.TestTemplate;
import org.neo4j.bolt.test.annotation.connection.SelectTransport;
import org.neo4j.bolt.test.connection.transport.FilteredTransportSelector;

/**
 * Marks an annotated function as a Bolt transport test which shall be executed across the range of supported Bolt
 * transport protocols.
 * <p />
 * When combined with {@link ProtocolTest}, the test matrix may be extended to include a set of different supported
 * protocol versions. When omitted, only the <em>latest supported protocol version</em> will be provided to the test
 * function.
 * <p />
 * This annotation implies {@link TestTemplate}. Note, however, that it may also be applied on class level in order to
 * configure a set of available transports for a set of tests. In that case, test functions should be annotated with
 * {@link TestTemplate} instead of repeating this annotation.
 */
@Documented
@TestTemplate
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@SelectTransport(FilteredTransportSelector.class)
public @interface TransportTest {}
