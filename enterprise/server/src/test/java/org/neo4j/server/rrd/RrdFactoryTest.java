/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rrd;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.ImpermanentGraphDatabase;
import org.neo4j.server.configuration.Configurator;
import org.rrd4j.core.RrdDb;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;

public class RrdFactoryTest
{
    private Configuration config;
    private ImpermanentGraphDatabase db;

    @Before
    public void setUp() throws Exception
    {
        config = new MapConfiguration( new HashMap<String, String>() );
        db = new ImpermanentGraphDatabase();
    }

    @After
    public void tearDown() throws Exception
    {
        try
        {
            db.shutdown();
        } catch ( Exception e )
        {
            ;
        }
    }

    @Test
    public void shouldTakeDirectoryLocationFromConfig() throws Exception
    {
        String expected = "target/rrd-test";
        config.addProperty( Configurator.RRDB_LOCATION_PROPERTY_KEY, expected );
        TestableRrdFactory factory = createRrdFactory();

        factory.createRrdDbAndSampler( db, new NullJobScheduler() );

        assertThat( factory.directoryUsed, is( expected ) );
    }


    @Test
    public void shouldCreateRrdInAGoodDefaultPlace() throws Exception
    {
        TestableRrdFactory factory = createRrdFactory();

        RrdDb rrdDbAndSampler = factory.createRrdDbAndSampler( db, new NullJobScheduler() );

        assertThat( factory.directoryUsed, startsWith( db.getStoreDir() ) );
        assertThat( factory.directoryUsed, endsWith( "rrd" ) );

        rrdDbAndSampler.close();
    }

    private TestableRrdFactory createRrdFactory()
    {
        return new TestableRrdFactory( config );
    }

    private static class TestableRrdFactory extends RrdFactory
    {
        public String directoryUsed;

        public TestableRrdFactory( Configuration config )
        {
            super( config );
        }

        @Override
        protected RrdDb createRrdb( String inDirectory, int stepSize, int stepsPerArchive,
                                    Sampleable... sampleables ) throws IOException
        {
            directoryUsed = inDirectory;
            return super.createRrdb( inDirectory, stepSize, stepsPerArchive, sampleables );
        }
    }

    private static class NullJobScheduler implements JobScheduler
    {
        @Override
        public void scheduleToRunEveryXSeconds( Job job, int runEveryXSeconds )
        {

        }
    }
}
