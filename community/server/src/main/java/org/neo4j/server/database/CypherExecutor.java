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
package org.neo4j.server.database;

import org.neo4j.cypher.javacompat.internal.ServerExecutionEngine;
import org.neo4j.kernel.impl.query.QueryExecutionEngine;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class CypherExecutor extends LifecycleAdapter
{
    private final Database database;
    private ServerExecutionEngine executionEngine;

    public CypherExecutor( Database database )
    {
        this.database = database;
    }

    public ServerExecutionEngine getExecutionEngine()
    {
        return executionEngine;
    }

    @Override
    public void start() throws Throwable
    {
        this.executionEngine = (ServerExecutionEngine) database.getGraph().getDependencyResolver()
                                                               .resolveDependency( QueryExecutionEngine.class );
    }

    @Override
    public void stop() throws Throwable
    {
        this.executionEngine = null;
    }
}
