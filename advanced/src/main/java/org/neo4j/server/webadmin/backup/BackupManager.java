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

import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

import org.neo4j.server.webadmin.domain.BackupFailedException;
import org.neo4j.server.webadmin.properties.ConfigFileFactory;
import org.quartz.CronExpression;
import org.quartz.CronTrigger;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;

/**
 * Handles scheduling of backup jobs.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public enum BackupManager
{

    INSTANCE;
 
    private boolean running = false;

    private BackupLog log;
    private BackupConfig config;

    private SchedulerFactory schedulerFactory = new StdSchedulerFactory();
    private Scheduler scheduler;

    public void start() throws IOException, SchedulerException,
            BackupFailedException
    {
        if ( !running )
        {
            log = new BackupLog( ConfigFileFactory.getBackupLogFile() );

            config = new BackupConfig( ConfigFileFactory.getBackupConfigFile() );

            log.deleteIrrelevantLogs( config );

            scheduler = schedulerFactory.getScheduler();

            scheduleBackups();

            running = true;

        }
    }

    public void stop() throws SchedulerException
    {
        if ( running )
        {
            running = false;
            scheduler.shutdown();
        }
    }

    public void restart() throws IOException, SchedulerException,
            BackupFailedException
    {
        stop();
        start();
    }

    public BackupConfig getConfig()
    {
        return config;
    }

    public BackupLog getLog()
    {
        return log;
    }

    public BackupJobDescription getJobDescription( Integer id )
    {
        return getConfig().getJobDescription( id );
    }

    //
    // INTERNALS
    //

    private void scheduleBackups() throws SchedulerException,
            BackupFailedException
    {

        for ( BackupJobDescription desc : config.getJobDescriptions() )
        {

            JobDataMap jobData = new JobDataMap();
            jobData.put( QuartzBackupJob.JOB_DESCRIPTION_KEY, desc );
            jobData.put( QuartzBackupJob.BACKUP_LOG_KEY, log );

            JobDetail jobDetail = new JobDetail( desc.getName(),
                    "Backup trigger group", QuartzBackupJob.class );
            jobDetail.setJobDataMap( jobData );

            CronTrigger trigger = new CronTrigger( desc.getName() + " Trigger",
                    "Backup trigger group" );

            try
            {
                CronExpression cron = new CronExpression(
                        desc.getCronExpression() );
                trigger.setCronExpression( cron );
                System.out.println( "Scheduling " + desc.getCronExpression() );
                scheduler.scheduleJob( jobDetail, trigger );
            }
            catch ( ParseException e )
            {
                log.logFailure( new Date(), desc,
                        "The cron expression is invalid." );
            }
        }

        scheduler.start();
    }
}
