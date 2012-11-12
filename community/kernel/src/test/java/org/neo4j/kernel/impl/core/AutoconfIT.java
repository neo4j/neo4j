/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import static java.lang.Character.isDigit;
import static java.lang.Character.toUpperCase;
import static java.lang.Long.parseLong;
import static java.util.regex.Pattern.quote;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.genericMap;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.Store;
import org.neo4j.kernel.impl.util.StringLogger;

public class AutoconfIT
{
    @Test
    public void autoConfResultsInMoreMemoryForBiggerStore() throws Exception
    {
        String storeDir = forTest( getClass() ).directory( "db", true ).getAbsolutePath();
        Map<Class<? extends Store>, Long> configForEmptyDb = getConfigForDb( storeDir );
        createBigDb( storeDir );
        Map<Class<? extends Store>, Long> configForBigDb = getConfigForDb( storeDir );
        for ( Map.Entry<Class<? extends Store>, Long> entry : configForEmptyDb.entrySet() )
            assertTrue( configForBigDb.get( entry.getKey() ).longValue() > entry.getValue().longValue() );
        
        long specificNodeBytes = 1234567;
        Map<Class<? extends Store>, Long> configForSpecific = getConfigForDb( storeDir,
                stringMap( "neostore.nodestore.db.mapped_memory", "" + specificNodeBytes ) );
        assertEquals( specificNodeBytes, configForSpecific.get( NodeStore.class ).longValue() );
    }

    private void createBigDb( String storeDir )
    {
        BatchInserter inserter = new BatchInserterImpl( storeDir );
        try
        {
            inserter.createNode( 100000000, null );
        }
        finally
        {
            inserter.shutdown();
        }
    }

    private Map<Class<? extends Store>,Long> getConfigForDb( String storeDir )
    {
        return getConfigForDb( storeDir, stringMap() );
    }
    
    private Map<Class<? extends Store>,Long> getConfigForDb( String storeDir, Map<String, String> config )
    {
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( storeDir, config );
        try
        {
            StringBuffer dumpBuffer = new StringBuffer();
            StringLogger dumpLogger = StringLogger.wrap( dumpBuffer );
            db.getDiagnosticsManager().extract( Config.class.getName(), dumpLogger );
            Map<String, String> readConfig = readDumpConfig( dumpBuffer.toString() );
            return genericMap(
                    NodeStore.class, asBytes( readConfig, "neostore.nodestore.db.mapped_memory" )
//                    RelationshipStore.class, asBytes( params.get( "neostore.relationshipstore.db.mapped_memory" ) ),
//                    PropertyStore.class, asBytes( params.get( "neostore.propertystore.db.mapped_memory" ) )
                    );
        }
        finally
        {
            db.shutdown();
        }
    }

    private Map<String, String> readDumpConfig( String dump )
    {
        Map<String,String> map = new HashMap<String, String>();
        for ( String line : dump.split( quote( "\n" ) ) )
        {
            line = line.trim();
            int equalIndex = line.indexOf( '=' );
            if ( equalIndex <= 0 ) continue;
            map.put( line.substring( 0, equalIndex ).trim(), line.substring( equalIndex+1, line.length() ).trim() );
        }
        return map;
    }

    private long asBytes( Map<String, String> readConfig, String key )
    {
        String conf = readConfig.get( key );
        if ( conf == null ) return 0;
        int factor = 1;
        char lastChar = conf.charAt( conf.length()-1 );
        if ( !isDigit( lastChar ) )
        {
            switch ( toUpperCase( lastChar ) )
            {
            case 'M': factor = 1024*1024; break;
            case 'G': factor = 1024*1024*1024; break;
            default: throw new IllegalArgumentException( "Unknown unit for '" + conf + "'" );
            }
            conf = conf.substring( 0, conf.length()-1 );
        }
        return parseLong( conf )*factor;
    }
}
