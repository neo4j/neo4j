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
import org.neo4j.bolt.test.annotation.wire.SelectWire;
import org.neo4j.bolt.test.wire.selector.FilteredBoltWireSelector;

/**
 * Marks an annotated function as a Bolt protocol test which shall be executed across a range of supported versions.
 * <p />
 * When combined with {@link TransportTest}, a transport test matrix may be extended to additionally include various
 * protocol versions.
 * <p />
 * This annotation implies {@link TestTemplate}. Note, however, that it may additionally be placed on class level in
 * order to configure the set of available protocol versions for all tests within a class. In that case, test functions
 * should be annotated with {@link TestTemplate} instead of repeating this annotation.
 */
@Documented
@TestTemplate
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@SelectWire(FilteredBoltWireSelector.class)
public @interface ProtocolTest {}
