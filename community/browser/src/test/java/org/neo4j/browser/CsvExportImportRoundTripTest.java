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
package org.neo4j.browser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.lang.String.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CsvExportImportRoundTripTest
{
    @Test
    public void verifyThatCsvImportedWithLoadCsvThenSerialisedWithBrowserJavascriptIsIdenticalToTheOriginal()
            throws Exception
    {
        Path inputCsv = filePath( "input.csv" );
        Path exportJavascript = filePath( "CsvExportImportRoundTripTest.js" );

        final GraphDatabaseService database = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Result cypherResult = database.execute( format( "LOAD CSV WITH HEADERS FROM 'file://%s' AS line " +
                "RETURN line.col1 AS col1, line.col2 as col2, line.col3 as col3", inputCsv ) );

        String outputCsvString = exportCsvUsingNodeJs( asJsonString( cypherResult ), exportJavascript );

        try ( BufferedReader reader = new BufferedReader( new FileReader( inputCsv.toFile() ) ) )
        {
            String inputCsvString = readFully( reader );

            assertEquals( inputCsvString, outputCsvString );
        }
    }

    private String asJsonString( Result cypherResult ) throws IOException
    {
        HashMap<String, Object> json = new HashMap<>();
        json.put( "columns", cypherResult.columns() );
        ArrayList<Collection<Object>> list = new ArrayList<>();
        while ( cypherResult.hasNext() )
        {
            Map<String, Object> cypherRow = cypherResult.next();
            ArrayList<Object> cells = new ArrayList<>();
            for ( String columnName : cypherResult.columns() )
            {
                cells.add( cypherRow.get( columnName ) );
            }
            list.add( cells );
        }
        json.put( "rows", list );

        return new ObjectMapper().writeValueAsString( json );
    }

    private Path filePath( String filePath ) throws URISyntaxException
    {
        URL resourceLoc = getClass().getClassLoader().getResource( filePath );
        assertNotNull( resourceLoc );
        return Paths.get( resourceLoc.toURI() );
    }

    private String exportCsvUsingNodeJs( String jsonString, Path path ) throws IOException, InterruptedException
    {
        ProcessBuilder processBuilder = new ProcessBuilder( "node", path.toString() );

        Process process = processBuilder.start();

        Writer in = new OutputStreamWriter( process.getOutputStream() );
        in.write( jsonString );
        in.close();

        int exitCode = process.waitFor();
        assertEquals( 0, exitCode );

        try ( BufferedReader reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) ) )
        {
            return readFully( reader );
        }
    }

    private String readFully( BufferedReader reader ) throws IOException
    {
        StringBuilder builder = new StringBuilder();
        String line;
        while ( (line = reader.readLine()) != null )
        {
            builder.append( line );
            builder.append( System.getProperty( "line.separator" ) );
        }
        return builder.toString();
    }
}
