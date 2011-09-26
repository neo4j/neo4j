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
package org.neo4j.kernel.impl.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;
import static org.neo4j.kernel.impl.util.StringLogger.getLogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.Test;

public class TestStringLogger
{
    @Test
    public void makeSureLogsAreRotated() throws Exception
    {
        String path = "target/test-data/stringlogger";
        deleteRecursively( new File( path ) );
        File logFile = new File( path, "messages.log" );
        File oldFile = new File( path, "messages.log.1" );
        File oldestFile = new File( path, "messages.log.2" );
        StringLogger logger = getLogger( path, 1 );
        assertFalse( oldFile.exists() );
        int counter = 0;
        String prefix = "Bogus message ";
        
        // First rotation
        while ( !oldFile.exists() )
        {
            logger.logMessage( prefix + counter++, true );
        }
        int mark1 = counter-1;
        logger.logMessage( prefix + counter++, true );
        assertTrue( firstLineOfFile( oldFile ).contains( prefix + "0" ) );
        assertTrue( lastLineOfFile( oldFile ).contains( prefix + mark1 ) );
        assertTrue( firstLineOfFile( logFile ).contains( prefix + (counter-1) ) );
        
        // Second rotation
        while ( !oldestFile.exists() )
        {
            logger.logMessage( prefix + counter++, true );
        }
        int mark2 = counter-1;
        logger.logMessage( prefix + counter++, true );
        assertTrue( firstLineOfFile( oldestFile ).contains( prefix + "0" ) );
        assertTrue( lastLineOfFile( oldestFile ).contains( prefix + mark1 ) );
        assertTrue( firstLineOfFile( oldFile ).contains( prefix + (mark1+1) ) );
        assertTrue( lastLineOfFile( oldFile ).contains( prefix + mark2 ) );
        assertTrue( firstLineOfFile( logFile ).contains( prefix + (counter-1) ) );
        
        // Third rotation, assert .2 file is now what used to be .1 used to be and
        // .3 doesn't exist
        long previousSize = 0;
        while ( true )
        {
            logger.logMessage( prefix + counter++, true );
            if ( logFile.length() < previousSize ) break;
            previousSize = logFile.length();
        }
        assertFalse( new File( path, "messages.log.3" ).exists() );
        assertTrue( firstLineOfFile( oldestFile ).contains( prefix + (mark1+1) ) );
        assertTrue( lastLineOfFile( oldestFile ).contains( prefix + mark2 ) );
    }

    private String firstLineOfFile( File file ) throws Exception
    {
        BufferedReader reader = new BufferedReader( new FileReader( file ) );
        String result = reader.readLine();
        reader.close();
        return result;
    }
    
    private String lastLineOfFile( File file ) throws Exception
    {
        BufferedReader reader = new BufferedReader( new FileReader( file ) );
        String line = null;
        String result = null;
        while ( (line = reader.readLine()) != null )
        {
            result = line;
        }
        reader.close();
        return result;
    }
}
