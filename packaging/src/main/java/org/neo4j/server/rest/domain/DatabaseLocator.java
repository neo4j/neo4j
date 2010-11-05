///**
// * Copyright (c) 2002-2010 "Neo Technology,"
// * Network Engine for Objects in Lund AB [http://neotechnology.com]
// *
// * This file is part of Neo4j.
// *
// * Neo4j is free software: you can redistribute it and/or modify
// * it under the terms of the GNU Affero General Public License as
// * published by the Free Software Foundation, either version 3 of the
// * License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU Affero General Public License for more details.
// *
// * You should have received a copy of the GNU Affero General Public License
// * along with this program. If not, see <http://www.gnu.org/licenses/>.
// */
//
//package org.neo4j.server.rest.domain;
//
//import java.io.File;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.PropertyContainer;
//import org.neo4j.index.IndexService;
//import org.neo4j.index.lucene.LuceneFulltextIndexService;
//import org.neo4j.index.lucene.LuceneIndexService;
//import org.neo4j.kernel.Config;
//import org.neo4j.kernel.EmbeddedGraphDatabase;
//import org.neo4j.remote.RemoteGraphDatabase;
//
///**
// * Handles instantiating a singleton GraphDatabaseService, as well as providing
// * access to various services around said database.
// * 
// * DatabaseLocator is able to both create a local database, but also to connect
// * to a remote database via RMI. Which option is chosen depends on what database
// * location has been defined, see {@link #setDatabaseLocation(String)}.
// * 
// * TODO: Refactor this to handle several databases at the same time, and to
// * provide a "shutdown and block"-type functionality. The latter is required to
// * perform backup foundations and other functions that need to be assured the
// * database is turned off while they operate.
// */
//public class DatabaseLocator
//{
//    /**
//     * If a system property with this key is defined when this class is first
//     * used, the value of that will be used as the default database location.
//     */
//    public static final String DB_LOCATION_PROPERTY_KEY = "org.neo4j.graphdb.location";
//
//    /**
//     * If no other location is set, this will be used to instantiate the
//     * database.
//     */
//    public static final String DEFAULT_DB_LOCATION = "target/neodb";
//
//    /**
//     * The underlying {@link GraphDatabase} for the REST server.
//     */
//    private static GraphDatabaseService db;
//    private static IndexService indexService;
//    private static IndexService fulltextIndexService;
//    private static Map<String, Index<? extends PropertyContainer>> indexes;
//
//    /**
//     * The URI to the underlying graph database. Can be either a local folder or
//     * an RMI URI.
//     */
//    private static String dbLocation = System.getProperty(
//            DB_LOCATION_PROPERTY_KEY, DEFAULT_DB_LOCATION );
//
//    private static String FILE_SCHEME = "file";
//
//    /**
//     * Set to true when using {@link #shutdownAndBlockGraphDatabase()}.
//     */
//    private static Boolean databaseIsBlocked = false;
//
//    public static GraphDatabaseService getGraphDatabase( URI baseUri )
//    {
//        try
//        {
//            blockCheck();
//        }
//        catch ( DatabaseBlockedException e )
//        {
//            // Wrapped here to retain backwards compatibility for this method.
//            throw new RuntimeException( e );
//        }
//
//        // TODO: this is just a KISS implementation
//        if ( db == null )
//        {
//            db = new EmbeddedGraphDatabase( getDatabaseLocation(), loadConfigFile() );
//        }
//        return db;
//    }
//
//    /**
//     * Get an instance of the graph database denoted by the system property
//     * "org.neo4j.graphdb.location".
//     * 
//     * The property can be an absolute or a relative path, which will lead to an
//     * EmbeddedGraphDatabase to be instantiated using that path.
//     * 
//     * It can also be an rmi adress to a remote graph database.
//     * 
//     * @return a GraphDatabaseService, freshly instantiated if this is the first
//     *         time this method is called.
//     * @throws DatabaseBlockedException if
//     *             {@link #shutdownAndBlockGraphDatabase()} has been called.
//     */
//    public static GraphDatabaseService getGraphDatabase()
//    {
//        blockCheck();
//        ensureDbIsAvailable();
//        return db;
//    }
//
//    /**
//     * Get the configuration object for the underlying database if it is
//     * supported. This will instantiate the database if this has not yet been
//     * done.
//     * 
//     * This will throw OperationNotSupportedException if the underlying database
//     * is not an embedded database (ie. if you have not specified a local file
//     * path as your DB_PATH).
//     * 
//     * Note: This will instantiate the database if it has not already been.
//     * 
//     * @return the configuration object
//     * @throws DatabaseBlockedException if
//     *             {@link #shutdownAndBlockGraphDatabase()} has been called.
//     */
//    public static Config getConfiguration() throws DatabaseBlockedException
//    {
//
//        if ( isLocalDatabase() )
//        {
//            return ( (EmbeddedGraphDatabase) getGraphDatabase() ).getConfig();
//        }
//        else
//        {
//            throw new UnsupportedOperationException(
//                    "Unable to access configuration on databases other than local ones." );
//        }
//    }
//
//    /**
//     * @return true if the database location specified is a local file.
//     */
//    public static boolean isLocalDatabase()
//    {
//
//        URI dbUri;
//        try
//        {
//            dbUri = new URI( getDatabaseLocation() );
//
//            return ( dbUri.getScheme() == null || dbUri.getScheme().equals(
//                    FILE_SCHEME ) );
//        }
//        catch ( URISyntaxException e )
//        {
//            throw new RuntimeException(
//                    "The database path specified is not a correct URI.", e );
//        }
//    }
//
//    /**
//     * Get index service. This is deprecated, please use
//     * {@link #getIndexService()} instead.
//     * 
//     * @param graphDb
//     * @return
//     */
//    @Deprecated
//    public static IndexService getIndexService( GraphDatabaseService graphDb )
//    {
//        if ( indexService == null )
//        {
//            indexService = new LuceneIndexService( graphDb );
//            fulltextIndexService = new LuceneFulltextIndexService( graphDb );
//            indexes = instantiateSomeDumbIndexes();
//        }
//        return indexService;
//    }
//
//    /**
//     * Get index service for the database. This will instantiate the database if
//     * that has not yet been done.
//     * 
//     * This method currently only works when using a local database. If you have
//     * specified an RMI URI as your database location, this will throw
//     * UnsupportedOperationException.
//     * 
//     * @return IndexService for the database given by
//     *         {@link #getGraphDatabase()}
//     * @throws DatabaseBlockedException if
//     *             {@link #shutdownAndBlockGraphDatabase()} has been called.
//     */
//    public static IndexService getIndexService()
//            throws DatabaseBlockedException
//    {
//        ensureIndexServiceIsAvailable();
//        return indexService;
//    }
//
//    public static Index<? extends PropertyContainer> getIndex(
//            GraphDatabaseService graphDb, String name )
//    {
//        // TODO Ensures instantiation... could be done better :)
//        getIndexService( graphDb );
//        Index<? extends PropertyContainer> index = indexes.get( name );
//        if ( index == null )
//        {
//            throw new RuntimeException( "No index '" + name + "'" );
//        }
//        return index;
//    }
//
//    /**
//     * @return the string URI of where the database is located.
//     */
//    public static String getDatabaseLocation()
//    {
//        return dbLocation;
//    }
//
//    /**
//     * Set the location of the database to work with. This can be a local file
//     * path, or a remote RMI URI. The first will lead to using an
//     * EmbeddedGraphDatabse, the latter will lead to using a
//     * RemoteGraphDatabase.
//     * 
//     * If you change this after having called {@link #getGraphDatabase()}, you
//     * need to call {@link #shutdownGraphDatabase()} to shut down the database
//     * started by that call. The change of dbLocation will not apply until the
//     * previous database is turned off.
//     */
//    public static void setDatabaseLocation( String dbLocation )
//    {
//        DatabaseLocator.dbLocation = dbLocation;
//    }
//
//    @Deprecated
//    public static void shutdownGraphDatabase( URI baseUri )
//    {
//        // TODO: this is just a KISS implementation
//        if ( fulltextIndexService != null )
//        {
//            fulltextIndexService.shutdown();
//            fulltextIndexService = null;
//        }
//        if ( indexService != null )
//        {
//            indexService.shutdown();
//            indexService = null;
//        }
//        if ( db != null )
//        {
//            db.shutdown();
//            db = null;
//        }
//    }
//
//    public static void shutdownGraphDatabase()
//    {
//        // TODO: this is just a KISS implementation
//        if ( fulltextIndexService != null )
//        {
//            fulltextIndexService.shutdown();
//            fulltextIndexService = null;
//        }
//        if ( indexService != null )
//        {
//            indexService.shutdown();
//            indexService = null;
//        }
//        if ( db != null )
//        {
//            db.shutdown();
//            db = null;
//        }
//    }
//
//    /**
//     * Used when shutting down the entire server. Will shut down the database,
//     * and make DatabaseLocator throw DatabaseBlockedExcetion when calling
//     * getGraphDatabase().
//     * 
//     * This is used, for instance, during server shutdown.
//     */
//    public static void shutdownAndBlockGraphDatabase()
//    {
//        databaseIsBlocked = true;
//        shutdownGraphDatabase();
//    }
//
//    /**
//     * Used to allow instantiating the database again after having used
//     * {@link #shutdownAndBlockGraphDatabase()}.
//     */
//    public static void unblockGraphDatabase()
//    {
//        databaseIsBlocked = false;
//    }
//
//    /**
//     * Check if the database is available.
//     * 
//     * @return true if the database is avaiable.
//     */
//    public static boolean databaseIsRunning()
//    {
//        return db != null;
//    }
//
//    //
//    // INTERNALS
//    //
//
//    private static Map<String, Index<? extends PropertyContainer>> instantiateSomeDumbIndexes()
//    {
//        Map<String, Index<? extends PropertyContainer>> map = new HashMap<String, Index<? extends PropertyContainer>>();
//        map.put( "node", new NodeIndex( indexService ) );
//        map.put( "fulltext-node", new NodeIndex( fulltextIndexService ) );
//        return map;
//    }
//
//    /**
//     * Check if database instantiation is blocked, throw DatabaseBloc
//     * 
//     * @throws DatabaseBlockedException
//     */
//    private static void blockCheck()
//    {
//        if ( databaseIsBlocked )
//        {
//            throw new DatabaseBlockedException(
//                    "Accessing the database is not allowed at the moment. This may be due to a server shutdown or other maintenence action." );
//        }
//    }
//
//    private static synchronized void ensureDbIsAvailable()
//    {
//        if ( db == null )
//        {
//
//            if ( isLocalDatabase() )
//            {
//                db = new EmbeddedGraphDatabase( getDatabaseLocation(),
//                        loadConfigFile() );
//            }
//            else
//            {
//                try
//                {
//                    db = new RemoteGraphDatabase( getDatabaseLocation() );
//                }
//                catch ( URISyntaxException e )
//                {
//                    throw new RuntimeException(
//                            "Unable to connect to database location '"
//                                    + getDatabaseLocation() + "'.", e );
//                }
//            }
//        }
//    }
//
//    private static synchronized void ensureIndexServiceIsAvailable()
//            throws DatabaseBlockedException
//    {
//        if ( indexService == null )
//        {
//            GraphDatabaseService db = getGraphDatabase();
//
//            if ( db instanceof EmbeddedGraphDatabase )
//            {
//                indexService = new LuceneIndexService( db );
//                fulltextIndexService = new LuceneFulltextIndexService( db );
//                indexes = instantiateSomeDumbIndexes();
//            }
//            else
//            {
//                // TODO: Indexing for remote dbs
//                throw new UnsupportedOperationException(
//                        "Indexing is not yet available in neo4j-rest for remote databases." );
//            }
//        }
//    }
//
//    private static Map<String, String> loadConfigFile()
//    {
//        File configFile = new File( new File( getDatabaseLocation() ),
//                "neo4j.properties" );
//        if ( configFile.exists() )
//        {
//            System.out.println( "Using configuration "
//                                + configFile.getAbsolutePath() );
//        }
//        return configFile.exists() ? EmbeddedGraphDatabase.loadConfigurations( configFile.getAbsolutePath() )
//                : new HashMap<String, String>();
//    }
//}
