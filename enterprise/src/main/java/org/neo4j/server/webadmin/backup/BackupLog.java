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

package org.neo4j.server.webadmin.backup;

import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;
import static org.neo4j.server.webadmin.utils.FileUtils.getFileAsString;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * KISS implementation of an in-memory backup log. The log keeps a list of log
 * entries for each backup job that is defined. The number of entries is limited
 * by {@link BackupLog#BACKLOG_LENGTH} per job, log entries are removed on a
 * FIFO basis.
 * 
 * The log is automatically loaded and persisted to disk using a JSON text file.
 * 
 * For each job configured, the log also makes sure to store the latest
 * successful job (if there ever was one), regardless of the number of failed
 * log entries.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public class BackupLog
{

    /**
     * Number of log statements to save per backup job.
     */
    public static final int BACKLOG_LENGTH = 25;

    public static final String LOG_KEY = "log";
    public static final String LATEST_SUCCESSFUL_KEY = "latestSuccess";

    private File logFile;
    private HashMap<Integer, ArrayList<BackupLogEntry>> mainLog = new HashMap<Integer, ArrayList<BackupLogEntry>>();
    private HashMap<Integer, BackupLogEntry> lastSuccessLog = new HashMap<Integer, BackupLogEntry>();

    public BackupLog( File logFile ) throws IOException
    {
        this.logFile = logFile;
        load();
    }

    public void logSuccess( Date date, BackupJobDescription description )

    {
        log( date, description, "Backup successful",
                BackupLogEntry.Type.SUCCESSFUL_BACKUP, -1 );
    }

    public void logFailure( Date date, BackupJobDescription description,
            String message )
    {
        log( date, description, message, BackupLogEntry.Type.ERROR, -1 );
    }

    public void logFailure( Date date, BackupJobDescription description,
            String message, int code )
    {
        log( date, description, message, BackupLogEntry.Type.ERROR, code );
    }

    public void logInfo( Date date, BackupJobDescription description,
            String message )
    {
        log( date, description, message, BackupLogEntry.Type.INFO, -1 );
    }

    public ArrayList<BackupLogEntry> getLog( int jobId )
    {
        if ( mainLog.containsKey( jobId ) )
        {
            return mainLog.get( jobId );
        }
        else
        {
            return new ArrayList<BackupLogEntry>();
        }
    }

    public BackupLogEntry getLatestSuccessful( int jobId )
    {
        if ( lastSuccessLog.containsKey( jobId ) )
        {
            return lastSuccessLog.get( jobId );
        }
        else
        {
            return null;
        }
    }

    public void deleteIrrelevantLogs( BackupConfig config ) throws IOException
    {
        Iterator<Integer> it = mainLog.keySet().iterator();
        while ( it.hasNext() )
        {
            if ( config.getJobDescription( it.next() ) == null )
            {
                it.remove();
            }
        }

        persist();
    }

    public Object serialize()
    {
        Map<String, Object> configMap = new HashMap<String, Object>();

        for ( Integer jobId : mainLog.keySet() )
        {
            if ( jobId != null )
            {
                ArrayList<Object> serializedLog = new ArrayList<Object>();
                for ( BackupLogEntry entry : mainLog.get( jobId ) )
                {
                    serializedLog.add( entry.serialize() );
                }

                Map<String, Object> jobMap = new HashMap<String, Object>();

                jobMap.put( LOG_KEY, serializedLog );
                jobMap.put(
                        LATEST_SUCCESSFUL_KEY,
                        lastSuccessLog.get( jobId ) != null ? lastSuccessLog.get(
                                jobId ).serialize()
                                : null );

                configMap.put( jobId.toString(), jobMap );
            }
        }

        return configMap;
    }

    //
    // INTERNALS
    //

    private synchronized void log( Date date, BackupJobDescription description,
            String message, BackupLogEntry.Type type, int code )
    {

        if ( !mainLog.containsKey( description.getId() ) )
        {
            mainLog.put( description.getId(), new ArrayList<BackupLogEntry>() );
        }

        BackupLogEntry logEntry = new BackupLogEntry( date, type, message,
                description.getId(), code );

        // Push stuff to main log for this job description
        ArrayList<BackupLogEntry> myLog = mainLog.get( description.getId() );
        myLog.add( 0, logEntry );

        while ( myLog.size() > BACKLOG_LENGTH )
        {
            myLog.remove( BACKLOG_LENGTH );
        }

        // Keep track of the latest successful job
        if ( type == BackupLogEntry.Type.SUCCESSFUL_BACKUP )
        {
            lastSuccessLog.put( description.getId(), logEntry );
        }

        try
        {
            persist();
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
    }

    private synchronized void persist() throws IOException
    {
        String json = createJsonFrom( serialize() );
        FileOutputStream configOut = new FileOutputStream( logFile );
        configOut.write( json.getBytes() );
        configOut.close();
    }

    @SuppressWarnings( "unchecked" )
    private synchronized void load() throws IOException
    {
        try
        {
            String raw = getFileAsString( logFile );

            if ( raw == null || raw.length() == 0 )
            {
                persist();
            }
            else
            {

                Map<String, Object> logMap = jsonToMap( raw );

                for ( String jobIdString : logMap.keySet() )
                {

                    int jobId = Integer.valueOf( jobIdString );

                    if ( !mainLog.containsKey( jobId ) )
                    {
                        mainLog.put( jobId, new ArrayList<BackupLogEntry>() );
                    }

                    Map<String, Object> jobMap = (Map<String, Object>) logMap.get( jobIdString );
                    ArrayList<BackupLogEntry> myLog = mainLog.get( jobId );

                    for ( Map<String, Object> serialLogEntry : (List<Map<String, Object>>) jobMap.get( LOG_KEY ) )
                    {
                        if ( serialLogEntry != null )
                        {
                            myLog.add( BackupLogEntry.deserialize( serialLogEntry ) );
                        }
                    }

                    if ( jobMap.get( LATEST_SUCCESSFUL_KEY ) != null )
                    {
                        lastSuccessLog.put(
                                jobId,
                                BackupLogEntry.deserialize( (Map<String, Object>) jobMap.get( LATEST_SUCCESSFUL_KEY ) ) );
                    }
                    else
                    {
                        lastSuccessLog.put( jobId, null );
                    }

                }

            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }
}
