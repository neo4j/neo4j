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
package org.neo4j.kernel.impl.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory.memoryMappingSetting;

public class AutoconfTest
{
    @Before
    public void given()
    {
        this.db = (ImpermanentGraphDatabase)new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @Test
    public void shouldConfigureDatabaseMemoryMappingAutomatically() throws Exception
    {
        // when
        Config config = db.getConfig();

        // then
        assertMemoryMappingAutoConfigured( config, "relationshipstore.db" );
        assertMemoryMappingAutoConfigured( config, "nodestore.db" );
        assertMemoryMappingAutoConfigured( config, "propertystore.db" );
        assertMemoryMappingAutoConfigured( config, "propertystore.db.strings" );
        assertMemoryMappingAutoConfigured( config, "propertystore.db.arrays" );
    }

    private ImpermanentGraphDatabase db;

    @After
    public void stopDb()
    {
        try
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
        finally
        {
            db = null;
        }
    }

    private void assertMemoryMappingAutoConfigured( Config config, String store )
    {
        Long configuredValue = config.get( memoryMappingSetting( "neostore." + store ) );
        assertTrue( format( "Memory mapping for '%s' should be greater than 0, was %s", store, configuredValue ),
                    configuredValue > 0 );
    }
}
