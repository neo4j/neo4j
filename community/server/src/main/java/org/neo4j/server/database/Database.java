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
package org.neo4j.server.database;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.ext.udc.UdcProperties;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.statistic.StatisticCollector;
import org.rrd4j.core.RrdDb;

public class Database
{
    public static Logger log = Logger.getLogger( Database.class );

    public AbstractGraphDatabase graph;

    private final String databaseStoreDirectory;
    private RrdDb rrdDb;
    private final StatisticCollector statisticCollector = new StatisticCollector();

    public Database( AbstractGraphDatabase db )
    {
        this.databaseStoreDirectory = db.getStoreDir();
        graph = db;
    }

    public Database( GraphDatabaseFactory factory, String databaseStoreDirectory )
    {
        this( createDatabase( factory, databaseStoreDirectory, null ) );
        log.warn(
                "No database tuning properties set in the property file, using defaults. Please specify the performance properties file with org.neo4j.server.db.tuning.properties in the server properties file [%s].",
                System.getProperty( "org.neo4j.server.properties" ) );
    }

    public Database( GraphDatabaseFactory factory, String databaseStoreDirectory,
            Map<String, String> databaseTuningProperties )
    {
        this( createDatabase( factory, databaseStoreDirectory, databaseTuningProperties ) );
    }

    private static AbstractGraphDatabase createDatabase( GraphDatabaseFactory factory, String databaseStoreDirectory,
            Map<String, String> databaseProperties )
    {
        log.info( "Using database at " + databaseStoreDirectory );

        if ( databaseProperties == null )
        {
            databaseProperties = new HashMap<String, String>();
        }

        putIfAbsent( databaseProperties, Config.ENABLE_REMOTE_SHELL, "true" );
        databaseProperties.put( Config.KEEP_LOGICAL_LOGS, "true" );
        databaseProperties.put( UdcProperties.UDC_SOURCE_KEY, "server" );

        return factory.createDatabase( databaseStoreDirectory, databaseProperties );
    }

    private static void putIfAbsent( Map<String, String> databaseProperties, String configKey, String configValue )
    {
        if ( databaseProperties.get( configKey ) == null )
        {
            databaseProperties.put( configKey, configValue );
        }
    }

    public void startup()
    {
        if ( graph != null )
        {
            log.info( "Successfully started database" );
        }
        else
        {
            log.error( "Failed to start database. GraphDatabaseService has not been properly initialized." );
        }
    }

    public void shutdown()
    {
        try
        {
            if ( rrdDb != null )
            {
                rrdDb.close();
            }
            if ( graph != null )
            {
                graph.shutdown();
            }
            log.info( "Successfully shutdown database" );
        }
        catch ( Exception e )
        {
            log.error( "Database did not shut down cleanly. Reason [%s]", e.getMessage() );
            throw new RuntimeException( e );
        }
    }

    public String getLocation()
    {
        return databaseStoreDirectory;
    }

    public org.neo4j.graphdb.index.Index<Relationship> getRelationshipIndex( String name )
    {
        RelationshipIndex index = graph.index()
                .forRelationships( name );
        if ( index == null )
        {
            throw new RuntimeException( "No index for [" + name + "]" );
        }
        return index;
    }

    public org.neo4j.graphdb.index.Index<Node> getNodeIndex( String name )
    {
        org.neo4j.graphdb.index.Index<Node> index = graph.index()
                .forNodes( name );
        if ( index == null )
        {
            throw new RuntimeException( "No index for [" + name + "]" );
        }
        return index;
    }

    public RrdDb rrdDb()
    {
        return rrdDb;
    }

    public void setRrdDb( RrdDb rrdDb )
    {
        this.rrdDb = rrdDb;
    }

    public IndexManager getIndexManager()
    {
        return graph.index();
    }

    public StatisticCollector statisticCollector()
    {
        return statisticCollector;
    }
}
