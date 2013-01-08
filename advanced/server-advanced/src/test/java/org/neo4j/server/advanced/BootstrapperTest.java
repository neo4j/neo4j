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
package org.neo4j.server.advanced;
/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.neo4j.server.advanced.jmx.ServerManagement;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.PropertyFileConfigurator;
import org.neo4j.server.helpers.ServerBuilder;

public class BootstrapperTest
{
    @Test
    public void shouldBeAbleToRestartServer() throws Exception
    {
        String dbDir1 = new File("target/db1").getAbsolutePath();
        
        // TODO: This needs to be here because of a startuphealthcheck
        // that requires this system property. Look into moving
        // startup health checks into bootstrapper to avoid this.
        File irellevant = new File("target/irellevant");
        irellevant.createNewFile();
        System.setProperty( "org.neo4j.server.properties", irellevant.getAbsolutePath());
        
        
        Configurator config = new PropertyFileConfigurator(
        		ServerBuilder
	        		.server()
	        		.usingDatabaseDir( dbDir1 )
	                .createPropertiesFiles());
        
        AdvancedNeoServer server = new AdvancedNeoServer(config);
        
        server.start( );
        
        assertNotNull( server.getDatabase().getGraph() );
        assertEquals( dbDir1, server.getDatabase().getGraph().getStoreDir() );

        // Change the database location
        String dbDir2 = new File("target/db2").getAbsolutePath();
        Configuration conf = config.configuration();
        conf.setProperty(Configurator.DATABASE_LOCATION_PROPERTY_KEY, dbDir2);
        
        ServerManagement bean = new ServerManagement( server );
        bean.restartServer();
        assertEquals( dbDir2, server.getDatabase().getGraph().getStoreDir() );
 
    }
}
