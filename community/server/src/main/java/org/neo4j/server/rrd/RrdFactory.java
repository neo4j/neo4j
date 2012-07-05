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
package org.neo4j.server.rrd;

import static java.lang.Double.NaN;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.server.configuration.Configurator.RRDB_LOCATION_PROPERTY_KEY;
import static org.rrd4j.ConsolFun.AVERAGE;
import static org.rrd4j.ConsolFun.MAX;
import static org.rrd4j.ConsolFun.MIN;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.server.database.Database;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.rrd.sampler.NodeIdsInUseSampleable;
import org.neo4j.server.rrd.sampler.PropertyCountSampleable;
import org.neo4j.server.rrd.sampler.RelationshipCountSampleable;
import org.rrd4j.ConsolFun;
import org.rrd4j.core.DsDef;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;
import org.rrd4j.core.RrdToolkit;

public class RrdFactory
{
    public static final int STEP_SIZE = 1;
    private static final String RRD_THREAD_NAME = "Statistics Gatherer";

    private final Configuration config;
    private static final Logger LOG = Logger.getLogger( RrdFactory.class );

    public RrdFactory( Configuration config )
    {

        this.config = config;
    }

    public RrdDb createRrdDbAndSampler( final Database db, JobScheduler scheduler )
    {
        Sampleable[] primitives = {
//                new MemoryUsedSampleable(),
                new NodeIdsInUseSampleable( db.getGraph() ),
                new PropertyCountSampleable( db.getGraph() ),
                new RelationshipCountSampleable( db.getGraph() )
        };

        Sampleable[] usage = {
//                new RequestBytesSampleable( db ),
//                new RequestMeanTimeSampleable( db ),
//                new RequestMedianTimeSampleable( db ),
//                new RequestMaxTimeSampleable( db ),
//                new RequestMinTimeSampleable( db ),
//                new RequestCountSampleable( db )
        };

        final String basePath = config.getString( RRDB_LOCATION_PROPERTY_KEY,
                getDefaultDirectory( (GraphDatabaseAPI) db.getGraph() ) );
        final RrdDb rrdb = createRrdb( basePath, join( primitives, usage ) );

        scheduler.scheduleAtFixedRate(
                new RrdJob( new RrdSamplerImpl( rrdb, primitives ) ),
                RRD_THREAD_NAME + "[primitives]",
                SECONDS.toMillis( 0 ),
                SECONDS.toMillis( 3 )
        );

//        scheduler.scheduleAtFixedRate(
//                new RrdJob( new RrdSamplerImpl( rrdb, usage )
//                {
//                    @Override
//                    public void updateSample()
//                    {
//                        db.statisticCollector().createSnapshot();
//                        super.updateSample();
//                    }
//                } ),
//                RRD_THREAD_NAME + "[usage]",
//                SECONDS.toMillis( 1 ),
//                SECONDS.toMillis( 60 )
//        );
        return rrdb;
    }

    private Sampleable[] join( Sampleable[]... sampleables )
    {
        ArrayList<Sampleable> result = new ArrayList<Sampleable>();
        for ( Sampleable[] sampleable : sampleables )
        {
            Collections.addAll( result, sampleable );
        }
        return result.toArray( new Sampleable[result.size()] );
    }

    private String getDefaultDirectory( GraphDatabaseAPI db )
    {
        return new File( db.getStoreDir(), "rrd" ).getAbsolutePath();
    }

    protected RrdDb createRrdb( String rrdPathx, Sampleable... sampleables )
    {
        File rrdFile = new File( rrdPathx );
        if ( rrdFile.exists() )
        {
            try
            {
                if ( !validateStepSize( rrdFile ) )
                {
                    return recreateArchive( rrdFile, sampleables );
                }

                Sampleable[] missing = checkDataSources( rrdFile.getAbsolutePath(), sampleables );
                if ( missing.length > 0 )
                {
                    updateDataSources( rrdFile.getAbsolutePath(), missing );
                }
                return new RrdDb( rrdFile.getAbsolutePath() );
            } catch ( IOException e )
            {
                // RRD file may have become corrupt
                LOG.error( "Unable to open rrd store, attempting to recreate it", e );
                return recreateArchive( rrdFile, sampleables );
            } catch (IllegalArgumentException e) {
                // RRD file may have become corrupt
                LOG.error( "Unable to open rrd store, attempting to recreate it", e );
                return recreateArchive( rrdFile, sampleables );
            }
        } else
        {
            RrdDef rrdDef = new RrdDef( rrdFile.getAbsolutePath(), STEP_SIZE );
            defineDataSources( rrdDef, sampleables );
            addArchives( rrdDef );
            try
            {
                return new RrdDb( rrdDef );
            } catch ( IOException e )
            {
                LOG.error( "Unable to create new rrd store", e );
                throw new RuntimeException( e );
            }
        }
    }

    private boolean validateStepSize( File rrdFile ) throws IOException
    {   
        RrdDb r = null; 
        try
        {
            r = new RrdDb( rrdFile.getAbsolutePath(), true );
            return ( r.getRrdDef().getStep() == STEP_SIZE );
        }
        finally
        {   
            if(r != null)
                r.close();
        }
    }

    private RrdDb recreateArchive( File rrdFile, Sampleable[] sampleables )
    {
        File file = new File( rrdFile.getParentFile(),
                rrdFile.getName() + "-invalid-" + System.currentTimeMillis() );

        if ( rrdFile.renameTo( file ) )
        {
            LOG.error( "current RRDB is invalid, renamed it to %s", file.getAbsolutePath() );
            return createRrdb( rrdFile.getAbsolutePath(), sampleables );
        }

        throw new RuntimeException( "RRD file ['" + rrdFile.getAbsolutePath()
                + "'] is invalid, but I do not have write permissions to recreate it." );
    }

    private static Sampleable[] checkDataSources( String rrdPath, Sampleable[] sampleables ) throws IOException
    {
        RrdDb rrdDb = new RrdDb( rrdPath, true );
        List<Sampleable> missing = new ArrayList<Sampleable>();
        for ( Sampleable sampleable : sampleables )
        {
            if ( rrdDb.getDatasource( sampleable.getName() ) == null )
            {
                missing.add( sampleable );
            }
        }
        rrdDb.close();
        return missing.toArray( new Sampleable[missing.size()] );
    }

    private static void updateDataSources( String rrdPath, Sampleable[] sampleables ) throws IOException
    {
        for ( Sampleable sampleable : sampleables )
        {
            LOG.warn( "Updating RRDB structure, adding: " + sampleable.getName() );
            RrdToolkit.addDatasource( rrdPath, createDsDef( sampleable ), true );
        }
    }

    private static DsDef createDsDef( Sampleable sampleable )
    {
        return new DsDef( sampleable.getName(), sampleable.getType(),
                120 * STEP_SIZE, NaN, NaN );
    }

    private void addArchives( RrdDef rrdDef )
    {
        for ( ConsolFun fun : asList( AVERAGE, MAX, MIN ) )
        {
            addArchive( rrdDef, fun, MINUTES.toSeconds( 30 ), SECONDS.toSeconds( 1 ) );
            addArchive( rrdDef, fun, DAYS.toSeconds( 1 ), MINUTES.toSeconds( 1 ) );
            addArchive( rrdDef, fun, DAYS.toSeconds( 7 ), MINUTES.toSeconds( 5 ) );
            addArchive( rrdDef, fun, DAYS.toSeconds( 30 ), MINUTES.toSeconds( 30 ) );
            addArchive( rrdDef, fun, DAYS.toSeconds( 1780 ), HOURS.toSeconds( 2 ) );
        }
    }

    private void addArchive( RrdDef rrdDef, ConsolFun fun, long length, long resolution )
    {
        rrdDef.addArchive( fun, 0.2,
                (int) ( resolution * STEP_SIZE ),
                (int) ( length / ( resolution * STEP_SIZE ) ) );
    }

    private void defineDataSources( RrdDef rrdDef, Sampleable[] sampleables )
    {
        for ( Sampleable sampleable : sampleables )
        {
            rrdDef.addDatasource( createDsDef( sampleable ) );
        }
    }

}
