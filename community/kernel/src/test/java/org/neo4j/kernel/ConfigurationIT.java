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
package org.neo4j.kernel;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.ImpermanentGraphDatabase;

public class ConfigurationIT {

	@Test
	public void shouldPickUpSystemProperties() 
	{
		System.setProperty(GraphDatabaseSettings.string_block_size.name(), "1337");
		GraphDatabaseAPI db = null;
		try
		{
			
			db = new ImpermanentGraphDatabase();
			int stringBlockSize = db.getDependencyResolver().resolveDependency(Config.class).get(GraphDatabaseSettings.string_block_size);
			
			assertThat(stringBlockSize, is(1337));
		}
		finally
		{
			if(db != null)
			{
				db.shutdown();
			}
			System.clearProperty(GraphDatabaseSettings.string_block_size.name());
		}
	}
	
}
