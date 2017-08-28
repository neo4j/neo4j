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
package org.neo4j.internal.kernel.api;

/**
 * A PlannerFrame exists from the start of Cypher query planning, until the last Graph interaction.
 * The PlannerFrame asserts that key assumptions made during planning still hold during execution, for example
 * that no indexes are deleted.
 *
 * When the query is planned and ready for execution, the beginRuntime() escalated the graph consistency guarantees
 * and essentially starts the real transaction.
 */
public interface PlannerFrame extends AutoCloseable
{
    Runtime beginRuntime();
}
