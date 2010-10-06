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

package org.neo4j.onlinebackup;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.TxModule;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;

/**
 * Command line tool to apply new logical logs.
 */
public class ApplyNewLogs
{
    private ApplyNewLogs()
    {
        // no instances
    }

    /**
     * Apply new logs.
     * 
     * @param args destination database path
     */
    public static void main( String args[] )
    {
        if ( args.length != 1 )
        {
            System.err.println( "Usage: ApplyNewLogs <db path>" );
            System.exit( -1 );
        }
        String destDir = args[0];
        if ( !new File( destDir ).exists() )
        {
            throw new RuntimeException(
                "Unable to locate store in[" + destDir + "]" );
        }
        Map<String,String> params = new HashMap<String, String>();
        params.put( "backup_slave", "true" );
        EmbeddedGraphDatabase graphDb = new EmbeddedGraphDatabase( destDir );
        setupLuceneIfOnClasspath( destDir, graphDb );
        XaDataSourceManager xaDsMgr = 
            graphDb.getConfig().getTxModule().getXaDataSourceManager();
        System.out.println( "Starting apply of new logs..." );
        for ( XaDataSource xaDs : xaDsMgr.getAllRegisteredDataSources() )
        {
            xaDs.makeBackupSlave();
            System.out.println( "Checking " + xaDs.getName() + " ..." );
            long nextVersion = xaDs.getCurrentLogVersion();
            boolean logApplied = false;
            while ( xaDs.hasLogicalLog( nextVersion ) )
            {
                try
                {
                    xaDs.applyLog( xaDs.getLogicalLog( nextVersion ) );
                    logApplied = true;
                }
                catch ( IOException e )
                {
                    throw new UnderlyingStorageException( 
                        "Unable to recover slave to consistent state", e );
                }
                nextVersion++;
            }
            if ( !logApplied )
            {
                System.out.println( "No new logs for " + xaDs.getName() ); 
            }
        }
        graphDb.shutdown();
        System.out.println( "Apply of new logs completed." );
    }

    private static final String LUCENE_DS_CLASS = 
        "org.neo4j.index.lucene.LuceneDataSource";

    private static final String LUCENE_FULLTEXT_DS_CLASS = 
        "org.neo4j.index.lucene.LuceneFulltextDataSource";
    
    private static void setupLuceneIfOnClasspath( String dstDir, 
            EmbeddedGraphDatabase graphDb )
    {
        TxModule txModule = graphDb.getConfig().getTxModule();
        XaDataSourceManager xaDsMgr = txModule.getXaDataSourceManager();
        // LockManager lockManager = graphDb.getConfig().getLockManager();
        try
        {
            // hack since kernel 1.0 unregisters datasources after recovery
            Class<?> clazz = Class.forName( LUCENE_FULLTEXT_DS_CLASS );
            if ( xaDsMgr.getXaDataSource( "lucene-fulltext" ) == null )
            {
                Map<Object, Object> params = new HashMap<Object, Object>();
                params.put( "dir", dstDir + "/lucene-fulltext" );
                txModule.registerDataSource( "lucene-fulltext", clazz.getName(), 
                        "162373".getBytes(), params );
            }
            clazz = Class.forName( LUCENE_DS_CLASS );
            if ( xaDsMgr.getXaDataSource( "lucene" ) == null )
            {
                Map<Object, Object> params = new HashMap<Object, Object>();
                params.put( "dir", dstDir + "/lucene" );
                txModule.registerDataSource( "lucene", clazz.getName(), 
                        "262374".getBytes(), params );
            }
        }
        catch ( ClassNotFoundException e )
        {
            // ok not on classpath
        }
    }
}
