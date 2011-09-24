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
package org.neo4j.kernel.impl.transaction.xaframework;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.KEEP_LOGICAL_LOGS;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.util.FileUtils;

public class TestContinueKeepLogs
{
    private static final String PATH = "target/test-data/keep-logs";
    
    @Before
    public void cleanDb() throws Exception
    {
        FileUtils.deleteRecursively( new File( PATH ) );
    }
    
    @Test
    public void dontKeepLogsAndDoStuffWithoutKeepLogsConfig() throws Exception
    {
        doStuffAndExpectLogs( dontKeepLogs() );
        doStuffAndExpectLogs( dontKeepLogs() );
    }
    
    @Test
    public void dontKeepLogsAndDoStuffWithKeepLogsConfig() throws Exception
    {
        doStuffAndExpectLogs( dontKeepLogs() );
        doStuffAndExpectLogs( keepLogs(), 0 );
    }
    
    @Test
    public void keepLogsAndDoStuffWithoutKeepLogsConfig() throws Exception
    {
        doStuffAndExpectLogs( keepLogs(), 0 );
        doStuffAndExpectLogs( dontConfigureKeepLogs(), 0, 1 );
    }
    
    @Test
    public void keepLogsForMeAndDoStuffWithoutKeepLogsConfig() throws Exception
    {
        doStuffAndExpectLogs( keepMyLogs(), 0 );
        doStuffAndExpectLogs( dontConfigureKeepLogs(), 0, 1 );
    }
    
    @Test
    public void keepLogsAndDoStuffWithPreventKeepLogsConfig() throws Exception
    {
        doStuffAndExpectLogs( keepLogs(), 0 );
        doStuffAndExpectLogs( dontKeepLogs(), 0 );
    }
    
    @Test
    public void keepLogsAndDoStuffWithPreventMeKeepLogsConfig() throws Exception
    {
        doStuffAndExpectLogs( keepLogs(), 0 );
        doStuffAndExpectLogs( dontKeepMyLogs(), 0 );
    }
    
    private Map<String, String> keepLogs()
    {
        return stringMap( KEEP_LOGICAL_LOGS, "true" );
    }
    
    private Map<String, String> dontKeepLogs()
    {
        return stringMap( KEEP_LOGICAL_LOGS, "false" );
    }
    
    private Map<String, String> dontKeepMyLogs()
    {
        return stringMap( KEEP_LOGICAL_LOGS, dataSourceName() + "=false" );
    }
    
    private Map<String, String> keepMyLogs()
    {
        return stringMap( KEEP_LOGICAL_LOGS, dataSourceName() + "=true" );
    }
    
    protected String dataSourceName()
    {
        return Config.DEFAULT_DATA_SOURCE_NAME;
    }

    private Map<String, String> dontConfigureKeepLogs()
    {
        return stringMap();
    }
    
    private void doStuffAndExpectLogs( Map<String, String> config, int... expectedLogVersions ) throws Exception
    {
        GraphDatabaseService db = new EmbeddedGraphDatabase( PATH, config );
        doTransaction( db );
        db.shutdown();
        expectLogs( db, expectedLogVersions );
    }
    
    protected File logDir( GraphDatabaseService db )
    {
        return new File( ((AbstractGraphDatabase)db).getStoreDir() );
    }

    private void expectLogs( GraphDatabaseService db, int... versions )
    {
        Set<Integer> versionSet = new HashSet<Integer>();
        for ( int version : versions ) versionSet.add( version );
        Pattern pattern = Pattern.compile( ".*\\.v\\d+" );
        for ( File file : logDir( db ).listFiles() )
        {
            if ( pattern.matcher( file.getName() ).matches() )
            {
                assertTrue( versionSet.remove( getLogVersion( file ) ) );
                System.out.println( "yo " + file );
            }
        }
        assertEquals( "Expected logs " + versionSet, 0, versionSet.size() );
    }

    private int getLogVersion( File file )
    {
        return Integer.parseInt( file.getName().substring( file.getName().indexOf( 'v' )+1 ) );
    }

    private void doTransaction( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        try
        {
            doTransactionWork( db );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    protected void doTransactionWork( GraphDatabaseService db )
    {
        db.createNode();
    }
}
