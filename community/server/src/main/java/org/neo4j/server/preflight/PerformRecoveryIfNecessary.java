/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.preflight;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.neo4j.kernel.impl.recovery.StoreRecoverer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.logging.Logger;

public class PerformRecoveryIfNecessary implements PreflightTask {
	
	private final Logger logger = Logger.getLogger( PerformRecoveryIfNecessary.class );
	private String failureMessage = "Unable to recover database";

	private Configuration config;
	private PrintStream out;
	private Map<String, String> dbConfig;

	public PerformRecoveryIfNecessary(Configuration serverConfig, Map<String,String> dbConfig, PrintStream out)
	{
		this.config = serverConfig;
		this.dbConfig = dbConfig;
		this.out = out;
	}
	
	@Override
	public boolean run() {
		try {
			File dbLocation = new File( config.getString( Configurator.DATABASE_LOCATION_PROPERTY_KEY ) );
			if(dbLocation.exists())
			{
				StoreRecoverer recoverer = new StoreRecoverer();
				
				if(recoverer.recoveryNeededAt(dbLocation, dbConfig))
				{
					out.println("Detected incorrectly shut down database, performing recovery..");
					recoverer.recover(dbLocation, dbConfig);
				}
			}
			
			return true;
		} catch(IOException e) {
			logger.error("Recovery startup task failed.", e);
			return false;
		}
	}

	@Override
	public String getFailureMessage() {
		return failureMessage;
	}

}
