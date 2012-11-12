/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;

import org.junit.Test;
import org.neo4j.server.Bootstrapper;
import org.neo4j.server.advanced.jmx.ServerManagement;
import org.neo4j.server.helpers.ServerBuilder;

public class BootstrapperTest
{
    @Test
    public void shouldBeAbleToRestartServer() throws Exception
    {
        String dbDir1 = new File("target/db1").getAbsolutePath();
        Bootstrapper bs = new AdvancedNeoServerBootstrapper();
        System.setProperty( "org.neo4j.server.properties",  ServerBuilder.server().usingDatabaseDir( dbDir1 )
                .createPropertiesFiles().getAbsolutePath() );
        bs.start( null );
        assertNotNull( bs.getServer().getDatabase().graph );
        assertEquals( dbDir1, bs.getServer().getDatabase().graph.getStoreDir() );

        String dbDir2 = new File("target/db2").getAbsolutePath();
        System.setProperty( "org.neo4j.server.properties",  ServerBuilder.server().usingDatabaseDir( dbDir2 )
                .createPropertiesFiles().getAbsolutePath() );
        ServerManagement bean = new ServerManagement( bs );
        bean.restartServer();
        assertEquals( dbDir2, bs.getServer().getDatabase().graph.getStoreDir() );
 
    }
}
