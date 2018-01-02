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
package org.neo4j.logging;

/**
 * Used to obtain a {@link Log} for a specified context
 */
public interface LogProvider
{
    /**
     * @param loggingClass the context for the returned {@link Log}
     * @return a {@link Log} that logs messages with the {@code loggingClass} as the context
     */
    Log getLog( Class loggingClass );

    /**
     * @param name the context for the returned {@link Log}
     * @return a {@link Log} that logs messages with the specified name as the context
     */
    Log getLog( String name );
}
