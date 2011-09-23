/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.rrd;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;

import static java.lang.Double.NaN;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class RrdFactoryTest
{
    private Configuration config;
    private Database db;

    @Before
    public void setUp() throws IOException
    {
        config = new MapConfiguration( new HashMap<String, String>() );
        db = new Database( new ImpermanentGraphDatabase() );
    }

    @After
    public void tearDown()
    {
        db.shutdown();
    }

    @Test
    public void shouldTakeDirectoryLocationFromConfig()
    {
        String expected = "target/rrd-test";
        config.addProperty( Configurator.RRDB_LOCATION_PROPERTY_KEY, expected );
        TestableRrdFactory factory = createRrdFactory();

        factory.createRrdDbAndSampler( db, new NullJobScheduler() );

        assertThat( factory.directoryUsed, is( expected ) );
    }

    @Test
    public void recreateDatabaseIfWrongStepsize()
    {
        String expected = "target/rrd-test";

        config.addProperty( Configurator.RRDB_LOCATION_PROPERTY_KEY, expected );
        TestableRrdFactory factory = createRrdFactory();

        factory.createRrdDbAndSampler( db, new NullJobScheduler() );

        assertThat( factory.directoryUsed, is( expected ) );
    }

    @Test
    public void shouldCreateRrdInAGoodDefaultPlace() throws IOException
    {
        TestableRrdFactory factory = createRrdFactory();

        // create invalid rrd
        File rrd = new File( db.graph.getStoreDir(), "rrd" );
        RrdDef rrdDef = new RrdDef( rrd.getAbsolutePath(), 3000 );
        rrdDef.addDatasource( "test", DsType.GAUGE, 1, NaN, NaN );
        rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 1, 1600 );
        RrdDb r = new RrdDb( rrdDef );
        r.close();

        RrdDb rrdDbAndSampler = factory.createRrdDbAndSampler( db, new NullJobScheduler() );

        assertThat( new File( factory.directoryUsed ).getParentFile()
                .list( new FilenameFilter()
                {
                    @Override
                    public boolean accept( File file, String s )
                    {
                        return s.startsWith( "rrd-invalid" );

                    }
                } ).length, is( 1 ) );

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
        protected RrdDb createRrdb( String inDirectory, Sampleable... sampleables )
        {
            directoryUsed = inDirectory;
            return super.createRrdb( inDirectory, sampleables );
        }
    }

    private static class NullJobScheduler implements JobScheduler
    {
        @Override
        public void scheduleAtFixedRate( Runnable job, String name, long delay, long period )
        {

        }
    }
}
