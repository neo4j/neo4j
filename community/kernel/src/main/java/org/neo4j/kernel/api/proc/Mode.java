/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.proc;

/**
 * The procedure mode affects how the procedure will execute, and which capabilities
 * it requires.
 */
public enum Mode
{
    /** This procedure will only perform read operations against the graph */
    READ_ONLY,
    /** This procedure may perform both read and write operations against the graph */
    READ_WRITE,
    /** This procedure may perform both read and write operations against the graph and create new tokens */
    TOKEN,
    /** This procedure will perform operations against the schema */
    SCHEMA_WRITE,
    /** This procedure will perform system operations - i.e. not against the graph */
    DBMS
}
