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
package org.neo4j.server.enterprise;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class EnterpriseDatabaseIT
{
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.cleanTestDirForTest( getClass() );

    @Test
    public void shouldStartInSingleModeByDefault() throws Throwable
    {
        EnterpriseDatabase db = new EnterpriseDatabase( new Configurator.Adapter()
        {
            @Override
            public Configuration configuration()
            {
                return new MapConfiguration( stringMap(
                        DATABASE_LOCATION_PROPERTY_KEY,
                        testDirectory.directory().getPath() ) );
            }
        } );

        try
        {
            db.start();

            assertThat( db.getGraph(), is( EmbeddedGraphDatabase.class ) );
        }
        finally
        {
            db.stop();
        }
    }
}
