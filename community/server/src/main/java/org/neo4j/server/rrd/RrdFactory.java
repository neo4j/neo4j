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
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.server.configuration.Configurator;
import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;

import javax.management.MalformedObjectNameException;
import java.io.File;
import java.io.IOException;

public class RrdFactory
{
    public static final int STEP_SIZE = 3000;
    public static final int STEPS_PER_ARCHIVE = 750;
    private final Configuration config;

    public RrdFactory( Configuration config )
    {

        this.config = config;
    }

    public RrdDb createRrdDbAndSampler( AbstractGraphDatabase db,
                                        JobScheduler scheduler ) throws MalformedObjectNameException, IOException
    {
        Sampleable[] sampleables = new Sampleable[]{
                new MemoryUsedSampleable(),
                new NodeIdsInUseSampleable( db ),
                new PropertyCountSampleable( db ),
                new RelationshipCountSampleable( db )
        };

        String basePath = config.getString( Configurator.RRDB_LOCATION_PROPERTY_KEY, getDefaultDirectory( db ) );
        RrdDb rrdb = createRrdb( basePath, STEP_SIZE, STEPS_PER_ARCHIVE, sampleables );

        RrdSampler sampler = new RrdSampler( rrdb.createSample(), sampleables );
        RrdJob job = new RrdJob( sampler );
        scheduler.scheduleToRunEveryXSeconds( job, 3 );
        return rrdb;
    }

    private String getDefaultDirectory( AbstractGraphDatabase db )
    {
        return new File( db.getStoreDir(), "rrd" ).getAbsolutePath();
    }

    protected RrdDb createRrdb( String inDirectory, int stepSize, int stepsPerArchive,
                                Sampleable... sampleables ) throws IOException
    {
        if ( !new File( inDirectory ).exists() )
        {
            RrdDef rrdDef = createRrdDb( inDirectory, stepSize );
            defineDataSources( stepSize, rrdDef, sampleables );
            addArchives( stepsPerArchive, rrdDef );
            return new RrdDb( rrdDef );
        } else
        {
            return new RrdDb( inDirectory );
        }
    }

    private static void addArchives( int stepsPerArchive, RrdDef rrdDef )
    {
        // Last 35 minutes
        rrdDef.addArchive( ConsolFun.AVERAGE, 0.5, 1, stepsPerArchive );

        // Last 6 hours
        rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 10, stepsPerArchive );

        // Last day
        rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 50, stepsPerArchive );

        // Last week
        rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 300, stepsPerArchive );

        // Last month
        rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 1300, stepsPerArchive );

        // Last five years
        rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 15000, stepsPerArchive * 5 );
    }

    private static void defineDataSources( int stepSize, RrdDef rrdDef, Sampleable[] sampleables )
    {
        for ( Sampleable sampleable : sampleables )
        {
            rrdDef.addDatasource( sampleable.getName(), DsType.GAUGE, stepSize, 0, Long.MAX_VALUE );

        }
    }

    private static RrdDef createRrdDb( String inDirectory, int stepSize )
    {
        RrdDef rrdDef = new RrdDef( inDirectory, stepSize );
        //rrdDef.setVersion( 2 );
        return rrdDef;
    }
}
