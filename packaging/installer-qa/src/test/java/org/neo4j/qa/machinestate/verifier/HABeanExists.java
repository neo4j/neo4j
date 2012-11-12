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
package org.neo4j.qa.machinestate.verifier;

import java.util.List;
import java.util.Map;

import org.neo4j.qa.driver.Neo4jDriver;
import org.neo4j.qa.machinestate.MachineModel;

public class HABeanExists implements Verifier {

	public HABeanExists(MachineModel[] machines) {
		// TODO Verify that the machines in this list 
		// are listed as parts of the cluster
	}

	@Override
	public void verify(Neo4jDriver driver) 
	{
		List<Map<String, Object>> jmx = driver.neo4jClient().getNeo4jJmxBeans();
		
		for(Map<String, Object> bean : jmx)
		{
			String name = (String) bean.get("name");
			if(name.contains("High Availability"))
			{
				return;
			}
		}
		
		throw new RuntimeException("Expected JMX to contain a High Availability bean, this seems to imply that the server is not in HA mode.");
	}

}
