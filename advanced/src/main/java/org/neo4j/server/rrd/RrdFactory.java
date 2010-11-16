/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;

import java.io.File;
import java.io.IOException;

public class RrdFactory
{

    public static RrdDb createRrdb( String inDirectory, int stepSize, int stepsPerArchive, Sampleable... sampleables ) throws IOException
    {
        if ( !new File( inDirectory ).exists() )
        {
            // CREATE RRD DEFINITION
            RrdDef rrdDef = new RrdDef( inDirectory, stepSize );

            rrdDef.setVersion( 2 );

            // DEFINE DATA SOURCES
            for ( Sampleable sampleable : sampleables )
            {
                rrdDef.addDatasource( sampleable.getName(), DsType.GAUGE, stepSize, 0, Long.MAX_VALUE );

            }
//
//            rrdDef.addDatasource( NODE_CACHE_SIZE, DsType.GAUGE, STEP_SIZE, 0, Long.MAX_VALUE );
//            rrdDef.addDatasource( NODE_COUNT, DsType.GAUGE, STEP_SIZE, 0, Long.MAX_VALUE );
//            rrdDef.addDatasource( RELATIONSHIP_COUNT, DsType.GAUGE, STEP_SIZE, 0, Long.MAX_VALUE );
//            rrdDef.addDatasource( PROPERTY_COUNT, DsType.GAUGE, STEP_SIZE, 0, Long.MAX_VALUE );
//            rrdDef.addDatasource( MEMORY_PERCENT, DsType.GAUGE, STEP_SIZE, 0, Long.MAX_VALUE );

            // DEFINE ARCHIVES

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

            // INSTANTIATE

            return new RrdDb( rrdDef );

        } else
        {
            return new RrdDb( inDirectory );
        }

    }
}
