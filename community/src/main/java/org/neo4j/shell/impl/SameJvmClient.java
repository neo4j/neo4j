/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.shell.impl;

import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellClient;
import org.neo4j.shell.ShellServer;

/**
 * An implementation of {@link ShellClient} optimized to use with a server
 * in the same JVM.
 */
public class SameJvmClient extends AbstractClient
{
	private Output out = new SystemOutput();
	private ShellServer server;
	private Session session = new SessionImpl();
	
	/**
	 * @param server the server to communicate with.
	 */
	public SameJvmClient( ShellServer server )
	{
		this.server = server;
		updateTimeForMostRecentConnection();
	}
	
	public Output getOutput()
	{
		return this.out;
	}

	public ShellServer getServer()
	{
		return this.server;
	}

	public Session session()
	{
		return this.session;
	}
	
	public void shutdown()
	{
	}
}
