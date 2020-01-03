/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.token.api;

public interface TokenConstants
{
    /**
     * Value indicating the a token does not exist.
     */
    int NO_TOKEN = -1;

    /**
     * Value indicating the a relationship type token does not exist.
     */
    int ANY_RELATIONSHIP_TYPE = NO_TOKEN;

    /**
     * Value indicating the a label token does not exist.
     */
    int ANY_LABEL = NO_TOKEN;

    /**
     * Value indicating the a property key token does not exist.
     */
    int ANY_PROPERTY_KEY = NO_TOKEN;
}
