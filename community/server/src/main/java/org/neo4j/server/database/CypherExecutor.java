/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.server.logging.Logger;

public class CypherExecutor implements Lifecycle
{
    public static Logger log = Logger.getLogger( CypherExecutor.class );

    private final Database database;
    private ExecutionEngine executionEngine;

    public CypherExecutor( Database database )
    {
        this.database = database;
    }

	public ExecutionEngine getExecutionEngine()
    {
		return executionEngine;
	}

	@Override
	public void init() throws Throwable 
	{
		
	}

	@Override
	public void start() throws Throwable 
	{
		this.executionEngine = new ExecutionEngine( database.getGraph() );
	}

	@Override
	public void stop() throws Throwable 
	{
		this.executionEngine = null;
	}

    @Override
	public void shutdown() throws Throwable
    {
        
    }
}
