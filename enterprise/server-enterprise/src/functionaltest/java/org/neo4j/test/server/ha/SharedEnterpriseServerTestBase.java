/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.test.server.ha;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4j.server.enterprise.EnterpriseNeoServer;
import org.neo4j.server.helpers.ServerHelper;

public class SharedEnterpriseServerTestBase
{
	private static boolean useExternal = Boolean.valueOf(System.getProperty("neo-server.external","false"));
	private static String externalURL = System.getProperty("neo-server.external.url","http://localhost:7474");

    protected static final EnterpriseNeoServer server()
    {
        return server;
    }

    /*
    protected static final void restartServer() throws IOException
    {
        releaseServer();
        ServerHolder.ensureNotRunning();
        allocateServer();
    }
    */

    protected final void cleanDatabase()
    {
    	if(useExternal) 
    	{
    		// TODO
    	} else
    	{
    		ServerHelper.cleanTheDatabase( server );
    	}
    }

    private static EnterpriseNeoServer server;
	private static String serverUrl;
	
	public static String getServerURL()
	{
		return serverUrl;
	}

    @BeforeClass
    public static void allocateServer() throws IOException
    {
    	if(useExternal) 
    	{
    		serverUrl = externalURL;
    	} else 
    	{
    		server = EnterpriseServerHolder.allocate();
    		serverUrl = server.baseUri().toString();
    	}
    }

    @AfterClass
    public static final void releaseServer()
    {
    	if(!useExternal) 
    	{
	        try
	        {
	            EnterpriseServerHolder.release( server );
	        }
	        finally
	        {
	            server = null;
	        }
    	}
    }
}
