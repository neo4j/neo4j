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
package org.neo4j.kernel.api;

public final class StatementConstants
{
    public static final int NO_SUCH_RELATIONSHIP_TYPE = -1;
    public static final int NO_SUCH_LABEL = -1;
    public static final int NO_SUCH_PROPERTY_KEY = -1;
    public static final long NO_SUCH_NODE = -1;
    public static final long NO_SUCH_RELATIONSHIP = -1;

    private StatementConstants()
    {
        throw new UnsupportedOperationException();
    }
}
