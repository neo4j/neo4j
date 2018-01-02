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
package org.neo4j.graphmatching.filter;

/**
 * Is either a regex leaf, that is, a real regex pattern or an abstraction of
 * two {@link FilterExpression}s which are ANDed or ORed together.
 */
@Deprecated
public interface FilterExpression
{
    /**
     * Matches a value from a {@code valueGetter} and returns whether or not
     * there was a match.
     * @param valueGetter the getter which fetches the value to match.
     * @return whether or not the value from {@code valueGetter} matches
     * the criterias found in this expression.
     */
    boolean matches( FilterValueGetter valueGetter );
}
