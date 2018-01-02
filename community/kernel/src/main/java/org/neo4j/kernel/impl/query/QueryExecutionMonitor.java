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
package org.neo4j.kernel.impl.query;

import java.util.Map;

/**
 * The current (December 2014) usage of this interface expects the {@code end*} methods to be idempotent.
 * That is, once either of them have been invoked with a particular session as parameter, invoking either
 * of them with the same session parameter should do nothing.
 */
public interface QueryExecutionMonitor
{
    void startQueryExecution( QuerySession session, String query, Map<String,Object> parameters );

    void endFailure( QuerySession session, Throwable failure );

    void endSuccess( QuerySession session );
}
