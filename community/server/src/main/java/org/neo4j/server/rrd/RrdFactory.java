/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.rrd4j.ConsolFun;
import org.rrd4j.core.DsDef;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;
import org.rrd4j.core.RrdToolkit;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.RrdDbWrapper;
import org.neo4j.server.rrd.sampler.NodeIdsInUseSampleable;
import org.neo4j.server.rrd.sampler.PropertyCountSampleable;
import org.neo4j.server.rrd.sampler.RelationshipCountSampleable;
import org.neo4j.server.web.ServerInternalSettings;

import static org.rrd4j.ConsolFun.AVERAGE;
import static org.rrd4j.ConsolFun.MAX;
import static org.rrd4j.ConsolFun.MIN;

import static java.lang.Double.NaN;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

public class RrdFactory
{
    public static final int STEP_SIZE = 1;
    private static final String RRD_THREAD_NAME = "Statistics Gatherer";

    private final Config config;
    private final Log log;

    public RrdFactory( Config config, LogProvider logProvider )
    {
        this.config = config;
        this.log = logProvider.getLog( getClass() );
    }

    public org.neo4j.server.database.RrdDbWrapper createRrdDbAndSampler( final Database db, JobScheduler scheduler )
            throws IOException
    {
        NeoStoresSupplier neoStore = db.getGraph().getDependencyResolver().resolveDependency( NeoStoresSupplier.class );
        AvailabilityGuard guard = db.getGraph().getDependencyResolver().resolveDependency( AvailabilityGuard.class );

        Sampleable[] primitives = {
                new NodeIdsInUseSampleable( neoStore, guard ),
                new PropertyCountSampleable( neoStore, guard ),
                new RelationshipCountSampleable( neoStore, guard )
        };

        Sampleable[] usage = {};

        File oldRrdFile = new File( db.getGraph().getStoreDir(), "rrd" );
        if ( oldRrdFile.exists() )
        {
            oldRrdFile.delete();
        }

        File rrdDir = config.get( ServerSettings.rrdb_location );
        if ( rrdDir == null )
        {
            Map<String,String> params = config.getParams();
            String rrdPath = config.get( ServerInternalSettings.rrd_store ).getAbsolutePath();
            params.put( ServerSettings.rrdb_location.name(), rrdPath );
            config.applyChanges( params );
            rrdDir = config.get( ServerSettings.rrdb_location );
        }

        File rrdFile = getRrdFile( db.getGraph(), rrdDir );
        final RrdDbWrapper rrdb = createRrdb( rrdFile, isEphemereal( db.getGraph() ), join( primitives, usage ) );

        scheduler.scheduleAtFixedRate(
                new RrdJob( new RrdSamplerImpl( rrdb.get(), primitives ) ),
                RRD_THREAD_NAME + "[primitives]",
                SECONDS.toMillis( 0 ),
                SECONDS.toMillis( 3 )
        );

        return rrdb;
    }

    private File getRrdFile( GraphDatabaseAPI db, File rrdDir ) throws IOException
    {
        if ( isEphemereal( db ) )
        {
            rrdDir = tempRrdDir();
        }

        if ( !rrdDir.exists() && !rrdDir.mkdirs() )
        {
            throw new IOException( "Cannot create directory " + rrdDir );
        }

        return new File( rrdDir, "rrd" );
    }

    protected File tempRrdDir() throws IOException
    {
        final File tempFile = File.createTempFile( "neo4j", "rrd" );
        if ( !tempFile.delete() )
        {
            throw new IOException( "cannot delete temp file: " + tempFile );
        }

        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    FileUtils.deleteRecursively( tempFile );
                }
                catch ( IOException e )
                {
                    // Ignore
                }
            }
        } );

        return tempFile;
    }

    private boolean isEphemereal( GraphDatabaseAPI db )
    {
        Config config = db.getDependencyResolver().resolveDependency( Config.class );
        if ( config == null )
        {
            return false;
        }
        else
        {
            Boolean ephemeral = config.get( GraphDatabaseFacadeFactory.Configuration.ephemeral );
            return ephemeral != null && ephemeral;
        }
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

    protected RrdDbWrapper createRrdb( File rrdFile, boolean ephemeral, Sampleable... sampleables )
    {
        if ( rrdFile.exists() )
        {
            try
            {
                if ( !validateStepSize( rrdFile ) )
                {
                    return recreateArchive( rrdFile, ephemeral, sampleables );
                }

                Sampleable[] missing = checkDataSources( rrdFile.getAbsolutePath(), sampleables );
                if ( missing.length > 0 )
                {
                    updateDataSources( rrdFile.getAbsolutePath(), missing );
                }
                return wrap( new RrdDb( rrdFile.getAbsolutePath() ), ephemeral );
            }
            catch ( IOException e )
            {
                // RRD file may have become corrupt
                log.error( "Unable to open rrd store, attempting to recreate it", e );
                return recreateArchive( rrdFile, ephemeral, sampleables );
            }
            catch ( IllegalArgumentException e )
            {
                // RRD file may have become corrupt
                log.error( "Unable to open rrd store, attempting to recreate it", e );
                return recreateArchive( rrdFile, ephemeral, sampleables );
            }
        }
        else
        {
            RrdDef rrdDef = new RrdDef( rrdFile.getAbsolutePath(), STEP_SIZE );
            defineDataSources( rrdDef, sampleables );
            addArchives( rrdDef );
            try
            {
                return wrap( new RrdDb( rrdDef ), ephemeral );
            }
            catch ( IOException e )
            {
                log.error( "Unable to create new rrd store", e );
                throw new RuntimeException( e );
            }
        }
    }

    private RrdDbWrapper wrap( RrdDb db, boolean ephemeral ) throws IOException
    {
        return ephemeral ? cleaningRrdDb( db ) : new RrdDbWrapper.Plain( db );
    }

    private RrdDbWrapper cleaningRrdDb( final RrdDb db )
    {
        return new RrdDbWrapper()
        {
            @Override
            public RrdDb get()
            {
                return db;
            }

            @Override
            public void close() throws IOException
            {
                try
                {
                    db.close();
                }
                finally
                {
                    new File( db.getPath() ).delete();
                }
            }
        };
    }

    private boolean validateStepSize( File rrdFile ) throws IOException
    {
        RrdDb r = null;
        try
        {
            r = new RrdDb( rrdFile.getAbsolutePath(), true );
            return (r.getRrdDef().getStep() == STEP_SIZE);
        }
        finally
        {
            if ( r != null )
            {
                r.close();
            }
        }
    }

    private RrdDbWrapper recreateArchive( File rrdFile, boolean ephemeral, Sampleable[] sampleables )
    {
        File file = new File( rrdFile.getParentFile(), rrdFile.getName() + "-invalid-" + System.currentTimeMillis() );
        try
        {
            FileUtils.moveFile( rrdFile, file );
        }
        catch (IOException e)
        {
            throw new RuntimeException( "RRD file ['" + rrdFile.getAbsolutePath()
                    + "'] is invalid, but I do not have write permissions to recreate it.", e );
        }

        log.error( "current RRDB is invalid, renamed it to %s", file.getAbsolutePath() );
        return createRrdb( rrdFile, ephemeral, sampleables );
    }

    private static Sampleable[] checkDataSources( String rrdPath, Sampleable[] sampleables ) throws IOException
    {
        RrdDb rrdDb = new RrdDb( rrdPath, true );
        List<Sampleable> missing = new ArrayList<>();
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

    private void updateDataSources( String rrdPath, Sampleable[] sampleables ) throws IOException
    {
        for ( Sampleable sampleable : sampleables )
        {
            log.warn( "Updating RRDB structure, adding: " + sampleable.getName() );
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
                (int) (resolution * STEP_SIZE),
                (int) (length / (resolution * STEP_SIZE)) );
    }

    private void defineDataSources( RrdDef rrdDef, Sampleable[] sampleables )
    {
        for ( Sampleable sampleable : sampleables )
        {
            rrdDef.addDatasource( createDsDef( sampleable ) );
        }
    }
}
