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
import org.neo4j.bolt.test.annotation.wire.SelectWire;
import org.neo4j.bolt.test.wire.selector.FilteredBoltWireSelector;
import org.neo4j.bolt.testing.annotation.Version;

/**
 * Selects a list of matching protocol versions for execution of one or more annotated test templates.
 * <p />
 * This is a meta-annotation.
 *
 * @see ExcludeWire to create a blacklist or filter matching versions from this list.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@SelectWire(FilteredBoltWireSelector.class)
@Target({ElementType.ANNOTATION_TYPE, ElementType.METHOD, ElementType.TYPE})
public @interface IncludeWire {

    /**
     * Selects a list of explicitly tested protocol versions.
     *
     * @return a set of versions.
     */
    Version[] value();
}
