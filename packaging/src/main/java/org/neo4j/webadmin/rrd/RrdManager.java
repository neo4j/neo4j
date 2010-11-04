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

package org.neo4j.webadmin.rrd;

import java.io.File;
import java.io.IOException;

import org.neo4j.server.NeoServer;
import org.rrd4j.ConsolFun;
import org.rrd4j.DsType;
import org.rrd4j.core.RrdDb;
import org.rrd4j.core.RrdDef;

/**
 * A singleton pre-configured round-robin database. This stores various data
 * points over time. As data points get older, they will be aggregated together,
 * enabling the round robin database to store data points over a massive
 * timespan in very little space.
 * 
 * Basically, the older the data gets, the more coarse grained it becomes, thus
 * taking less space.
 * 
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class RrdManager
{

    public static final String RRDB_FILENAME = "neo4j.rrdb";

    // DATA SOURCE HANDLES

    public static final String NODE_CACHE_SIZE = "node_cache_size";
    public static final String NODE_COUNT = "node_count";
    public static final String RELATIONSHIP_COUNT = "relationship_count";
    public static final String PROPERTY_COUNT = "property_count";

    public static final String MEMORY_PERCENT = "memory_usage_percent";

    public static final long STEP_SIZE = 3000;
    public static final int STEPS_PER_ARCHIVE = 750;

    /**
     * Singleton instance of central round robin database.
     */
    private static RrdDb INSTANCE;
    

    public static RrdDb getRrdDB()
    {
        if ( INSTANCE == null )
        {

            try
            {
                if ( !new File( getDbFilePath() ).exists() )
                {
                    // CREATE RRD DEFINITION
                    RrdDef rrdDef = new RrdDef( getDbFilePath(), STEP_SIZE );

                    rrdDef.setVersion( 2 );

                    // DEFINE DATA SOURCES

                    rrdDef.addDatasource( NODE_CACHE_SIZE, DsType.GAUGE,
                            STEP_SIZE, 0, Long.MAX_VALUE );

                    rrdDef.addDatasource( NODE_COUNT, DsType.GAUGE, STEP_SIZE,
                            0, Long.MAX_VALUE );

                    rrdDef.addDatasource( RELATIONSHIP_COUNT, DsType.GAUGE,
                            STEP_SIZE, 0, Long.MAX_VALUE );

                    rrdDef.addDatasource( PROPERTY_COUNT, DsType.GAUGE,
                            STEP_SIZE, 0, Long.MAX_VALUE );

                    rrdDef.addDatasource( MEMORY_PERCENT, DsType.GAUGE,
                            STEP_SIZE, 0, Long.MAX_VALUE );

                    // DEFINE ARCHIVES

                    // Last 35 minutes
                    rrdDef.addArchive( ConsolFun.AVERAGE, 0.5, 1,
                            STEPS_PER_ARCHIVE );

                    // Last 6 hours
                    rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 10,
                            STEPS_PER_ARCHIVE );

                    // Last day
                    rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 50,
                            STEPS_PER_ARCHIVE );

                    // Last week
                    rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 300,
                            STEPS_PER_ARCHIVE );

                    // Last month
                    rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 1300,
                            STEPS_PER_ARCHIVE );

                    // Last five years
                    rrdDef.addArchive( ConsolFun.AVERAGE, 0.2, 15000,
                            STEPS_PER_ARCHIVE * 5 );

                    // INSTANTIATE

                    INSTANCE = new RrdDb( rrdDef );

                }
                else
                {
                    INSTANCE = new RrdDb( getDbFilePath() );
                }
            }
            catch ( IOException e )
            {
                throw new RuntimeException(
                        "IO Error trying to access round robin database path. See nested exception.",
                        e );
            }
        }

        return INSTANCE;
    }

    //
    // INTERNALS
    //

    /**
     * Get database path. Create any missing folders on the path.
     */
    public static String getDbFilePath() throws IOException
    {
        File dbPath = new File(
                NeoServer.INSTANCE.configuration().getString( NeoServer.WEBADMIN_NAMESPACE+ "rrdb.location"  ));

        if ( !dbPath.exists() && !dbPath.mkdirs() )
        {
            throw new IllegalStateException(
                    "Unable to use round-robin path '" + dbPath.toString()
                            + "'. Does user have write permissions?" );
        }

        return new File( dbPath, RRDB_FILENAME ).getAbsolutePath();
    }
}
