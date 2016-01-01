/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.query;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Settings;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class QueryLoggerIT
{

    @Rule
    public final EphemeralFileSystemRule fileSystem = new EphemeralFileSystemRule();
    @Rule
    public final TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );
    private GraphDatabaseBuilder databaseBuilder;
    public static final String QUERY = "CREATE (n:Foo{bar:\"baz\"})";

    @Before
    public void setUp()
    {
        databaseBuilder = new TestGraphDatabaseFactory().setFileSystem( fileSystem.get() )
                .newImpermanentDatabaseBuilder( testDirectory.graphDbDir().getAbsolutePath() );
    }

    @Test
    public void shouldLogQuerySlowerThanThreshold() throws Exception
    {
        final File logFilename = new File( testDirectory.graphDbDir(), "queries.log" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.log_queries_filename, logFilename.getPath() )
                .newGraphDatabase();

        executeQueryAndShutdown( database );

        List<String> logLines = readAllLines( logFilename );
        assertEquals( 1, logLines.size() );
        assertThat( logLines.get( 0 ), Matchers.endsWith( String.format( " ms: %s - %s",
                QueryEngineProvider.embeddedSession(), QUERY ) ) );
    }

    @Test
    public void shouldSuppressQueryLoggingIfTheGivenPathIsNull() throws Exception
    {
        final File logFilename = new File( testDirectory.graphDbDir(), "messages.log" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .newGraphDatabase();

        executeQueryAndShutdown( database );

        List<String> lines = readAllLines( logFilename );
        boolean found = false;
        for ( String line : lines )
        {
            if ( line.endsWith( GraphDatabaseSettings.log_queries.name() +
                                " is enabled but no " +
                                GraphDatabaseSettings.log_queries_filename.name() +
                                " has not been provided in configuration, hence query logging is suppressed" ) )
            {
                found = true;
            }
        }
        assertTrue( found );
    }

    @Test
    public void disabledQueryLogging() throws IOException
    {
        final File logFilename = new File( testDirectory.graphDbDir(), "queries.log" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.FALSE )
                .setConfig( GraphDatabaseSettings.log_queries_filename, logFilename.getPath() )
                .newGraphDatabase();

        executeQueryAndShutdown( database );

        List<String> lines = readAllLines( logFilename );
        assertTrue( "Should not have any queries logged since query log is disabled", lines.isEmpty() );
    }

    @Test
    public void queryLogRotation() throws Exception
    {
        final File logFilename = new File( testDirectory.graphDbDir(), "queries.log" );
        final File shiftedLogFilename = new File( testDirectory.graphDbDir(), "queries.log.1" );
        GraphDatabaseService database = databaseBuilder.setConfig( GraphDatabaseSettings.log_queries, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.log_queries_filename, logFilename.getPath() )
                .setConfig( GraphDatabaseSettings.log_queries_rotation_threshold, "1M" )
                .newGraphDatabase();


        String longQuery = createLongQuery();
        database.execute( longQuery );
        database.execute( longQuery );
        // execute one more slow query which should go to new log file
        database.execute( longQuery );
        database.shutdown();

        List<String> lines = readAllLines( logFilename );
        assertEquals( 1, lines.size() );
        List<String> shiftedLines = readAllLines( shiftedLogFilename );
        assertEquals( 2, shiftedLines.size() );
    }

    private String createLongQuery()
    {
        String longIdentifier = getStringWithLenght( 1024 * 1024 );
        return "CREATE (n:Foo{bar:\"" + longIdentifier + "\"})";
    }

    private String getStringWithLenght(int size)
    {
        char[] chars = new char[size];
        Arrays.fill(chars, 'a');
        return new String( chars );
    }


    private void executeQueryAndShutdown( GraphDatabaseService database )
    {
        database.execute( QUERY );
        database.shutdown();
    }

    private List<String> readAllLines( File logFilename ) throws IOException
    {
        List<String> logLines = new ArrayList<>();
        try ( BufferedReader reader = new BufferedReader( fileSystem.get().openAsReader( logFilename, "UTF-8" ) ) )
        {
            for ( String line; (line = reader.readLine()) != null; )
            {
                logLines.add( line );
            }
        }

        return logLines;
    }
}
