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
package org.neo4j.shell;

import java.rmi.RemoteException;

import org.neo4j.shell.apps.extra.Gsh;
import org.neo4j.shell.apps.extra.Jsh;
import org.neo4j.shell.impl.AbstractAppServer;

/**
 * A common concrete implement of an {@link AppShellServer} which contains
 * default packages and exit app.
 */
public class SimpleAppServer extends AbstractAppServer
{
	/**
	 * Creates a new simple app server and adds default packages.
	 * @throws RemoteException RMI error.
	 */
	public SimpleAppServer() throws RemoteException
	{
		super();
	}

    @Deprecated
    protected void addStandardApps()
    {
    }

    @Deprecated
    protected void addExtraApps()
    {
        addApp( Gsh.class );
        addApp( Jsh.class );
    }

    @Deprecated
    protected App findBuiltInApp( String command )
    {
        return null;
    }

    @Override
    public App findApp( String command )
    {
        App app = this.findBuiltInApp( command );
        return app != null ? app : super.findApp( command );
    }
}
