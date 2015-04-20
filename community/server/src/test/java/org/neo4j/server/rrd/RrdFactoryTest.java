/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.RrdDbWrapper;
import org.neo4j.server.database.WrappedDatabase;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.Mute;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.Double.NaN;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.test.Mute.muteAll;

public class RrdFactoryTest
{
    private Config config;
    private Database db;

    TargetDirectory target = TargetDirectory.forTest( RrdFactoryTest.class );

    @Rule
    public TargetDirectory.TestDirectory testDirectory = target.testDirectory();
    @Rule
    public Mute mute = muteAll();

    @Before
    public void setUp() throws IOException
    {
        db = new WrappedDatabase( new ImpermanentGraphDatabase(
                TargetDirectory.forTest( getClass() ).cleanDirectory( "rrd" ).getAbsolutePath()) );
        config = new Config();
    }

    @After
    public void tearDown()
    {
        db.getGraph().shutdown();
    }

    @Test
    public void shouldTakeDirectoryLocationFromConfig() throws Exception
    {
        String expected = testDirectory.directory().getAbsolutePath();
        addProperty( Configurator.RRDB_LOCATION_PROPERTY_KEY, expected );
        TestableRrdFactory factory = createRrdFactory();

        factory.createRrdDbAndSampler( db, new NullJobScheduler() );

        assertThat( factory.directoryUsed, is( expected ) );
    }

    private void addProperty( String rrdbLocationPropertyKey, String expected )
    {
        Map<String, String> params = config.getParams();
        params.put( rrdbLocationPropertyKey, expected );
        config.applyChanges( params );
    }

    @Test
    public void recreateDatabaseIfWrongStepsize() throws Exception
    {
        String expected = testDirectory.directory().getAbsolutePath();

        addProperty( Configurator.RRDB_LOCATION_PROPERTY_KEY, expected );
        TestableRrdFactory factory = createRrdFactory();

        factory.createRrdDbAndSampler( db, new NullJobScheduler() );

        assertThat( factory.directoryUsed, is( expected ) );
    }

    @Test
    public void shouldMoveAwayInvalidRrdFile() throws IOException
    {
        //Given
        String expected = new File( testDirectory.directory(), "rrd-test").getAbsolutePath();
        addProperty( Configurator.RRDB_LOCATION_PROPERTY_KEY, expected );

        TestableRrdFactory factory = createRrdFactory();
        createInvalidRrdFile( expected );


        //When
        RrdDbWrapper rrdDbAndSampler = factory.createRrdDbAndSampler( db, new NullJobScheduler() );


        //Then
        assertSubdirectoryExists( "rrd-test-invalid", factory.directoryUsed );

        rrdDbAndSampler.close();
    }

    private void createInvalidRrdFile( String expected ) throws IOException
    {
        // create invalid rrd
        File rrd = new File( expected );
        RrdDef rrdDef = new RrdDef( rrd.getAbsolutePath(), 3000 );
        rrdDef.addDatasource( "test", DsType.GAUGE, 1, NaN, NaN );
        rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 1, 1600 );
        RrdDb r = new RrdDb( rrdDef );
        r.close();
    }

    @Test
    public void shouldCreateRrdFileInTempLocationForImpermanentDatabases() throws IOException
    {
        // Given
        String expected = testDirectory.directory().getAbsolutePath();
        TestableRrdFactory factory = createRrdFactory( expected );

        // When
        factory.createRrdDbAndSampler( db, new NullJobScheduler() );

        // Then
        assertThat( factory.directoryUsed, is( expected ) );
    }

    @Test
    public void shouldCreateRrdFileInDbSubdirectory() throws Exception
    {
        String storeDir = testDirectory.directory().getAbsolutePath();
        db = new WrappedDatabase( (InternalAbstractGraphDatabase)
                new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDir ) );
        TestableRrdFactory factory = createRrdFactory();

        // When
        factory.createRrdDbAndSampler( db, new NullJobScheduler() );

        //Then
        String rrdParent = new File( factory.directoryUsed ).getParent();

        assertThat( rrdParent, is( storeDir ) );
    }


    private void assertSubdirectoryExists( final String directoryThatShouldExist, String directoryUsed )
    {
        File parentFile = new File( directoryUsed ).getParentFile();
        String[] list = parentFile.list();

        for ( String aList : list )
        {
            if (aList.startsWith( directoryThatShouldExist ))
            {
                return;
            }
        }

        fail( String.format( "Didn't find [%s] in [%s]", directoryThatShouldExist, directoryUsed ) );
    }

    private TestableRrdFactory createRrdFactory()
    {
        return new TestableRrdFactory( config, new File( testDirectory.directory(), "rrd" ).getAbsolutePath() );
    }

    private TestableRrdFactory createRrdFactory( String tempRrdFile )
    {
        return new TestableRrdFactory( config, tempRrdFile );
    }

    private static class TestableRrdFactory extends RrdFactory
    {
        public String directoryUsed;
        private final String tempRrdFile;

        public TestableRrdFactory( Config config, String tempRrdFile )
        {
            super( config, DevNullLoggingService.DEV_NULL );
            this.tempRrdFile = tempRrdFile;
        }

        @Override
        protected String tempRrdFile() throws IOException
        {
            return tempRrdFile;
        }

        @Override
        protected RrdDbWrapper createRrdb( File inDirectory, boolean ephemeral, Sampleable... sampleables )
        {
            directoryUsed = inDirectory.getAbsolutePath();
            return super.createRrdb( inDirectory, ephemeral, sampleables );
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
