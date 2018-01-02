/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphmatching;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * An interface which abstracts value matching. Examples of value matchers would be:
 * exact matchi, regex match a.s.o. Also look at {@link CommonValueMatchers}.
 */
@Deprecated
public interface ValueMatcher
{
    /**
     * Tries to match {@code value} to see if it matches an expected value.
     * {@code value} is {@code null} if the property wasn't found on the
     * {@link Node} or {@link Relationship} it came from.
     * 
     * The value can be of type array, where {@link ArrayPropertyUtil} can be of
     * help.
     * 
     * @param value the value from a {@link Node} or {@link Relationship} to
     *            match against an expected value.
     * @return {@code true} if the value matches, else {@code false}.
     */
    boolean matches( Object value );
}
