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
package org.neo4j.bolt.test.annotation.wire.selector;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.SelectWire;
import org.neo4j.bolt.test.wire.selector.DefaultBoltWireSelector;

/**
 * Selects only the default wire implementation for the invocations of a given test template.
 * <p />
 * This annotation is primarily intended for use in conjunction with {@link ProtocolTest} in order to override the
 * selection behavior in order to only select the most recent protocol revision. This may be useful for test cases which
 * are otherwise too expensive in order to operate on all supported revisions.
 * <p/>
 * This is a meta-annotation.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@SelectWire(DefaultBoltWireSelector.class)
@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.TYPE})
public @interface LatestOnly {}
