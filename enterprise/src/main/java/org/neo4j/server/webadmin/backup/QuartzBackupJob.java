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

import java.io.File;
import java.util.Date;

import org.neo4j.rest.domain.DatabaseBlockedException;
import org.neo4j.server.webadmin.domain.BackupFailedException;
import org.neo4j.server.webadmin.domain.NoBackupFoundationException;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class QuartzBackupJob implements Job
{

    public static final String BACKUP_LOG_KEY = "backupLog";
    public static final String JOB_DESCRIPTION_KEY = "jobDescription";

    public synchronized void execute( JobExecutionContext ctx )
            throws JobExecutionException
    {
        BackupLog log = (BackupLog) ctx.getJobDetail().getJobDataMap().get(
                BACKUP_LOG_KEY );
        BackupJobDescription jobDesc = (BackupJobDescription) ctx.getJobDetail().getJobDataMap().get(
                JOB_DESCRIPTION_KEY );

        log.logInfo( new Date(), jobDesc, "started" );

        try
        {
            BackupPerformer.doBackup( new File( jobDesc.getPath() ) );
            log.logSuccess( new Date(), jobDesc );
        }
        catch ( NoBackupFoundationException e )
        {
            if ( jobDesc.isAutoFoundation() )
            {
                try
                {
                    BackupPerformer.doBackupFoundation( new File(
                            jobDesc.getPath() ) );
                    BackupPerformer.doBackup( new File( jobDesc.getPath() ) );
                    log.logSuccess( new Date(), jobDesc );
                }
                catch ( BackupFailedException e1 )
                {
                    e1.printStackTrace();
                    log.logFailure(
                            new Date(),
                            jobDesc,
                            "Unable to create backup foundation, make sure I have write permissions to '"
                                    + ( new File( jobDesc.getPath() ).getAbsolutePath() )
                                    + "'." );
                }
                catch ( NoBackupFoundationException e1 )
                {
                    e1.printStackTrace();
                    log.logFailure( new Date(), jobDesc,
                            "Fatal: Tried to create backup foundation, but failed: "
                                    + e1.getMessage() );
                }
                catch ( DatabaseBlockedException e1 )
                {
                    log.logFailure( new Date(), jobDesc,
                            "Backup failed: Database is manually blocked (is server shutting down?)." );
                }
            }
            else
            {
                log.logFailure( new Date(), jobDesc,
                        "Needs manual backup foundation.",
                        BackupLogEntry.NEED_FOUNDATION_CODE );
            }
        }
        catch ( BackupFailedException e )
        {
            log.logFailure( new Date(), jobDesc,
                    "Backup failed: " + e.getMessage() );
        }
        catch ( DatabaseBlockedException e )
        {
            log.logFailure( new Date(), jobDesc,
                    "Backup failed: Database is manually blocked (is server shutting down?)." );
        }
        catch ( Exception e )
        {
            // Pokémon catch
            e.printStackTrace();
        }
    }
}
