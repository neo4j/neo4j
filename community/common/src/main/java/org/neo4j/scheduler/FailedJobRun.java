/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.scheduler;

import java.time.Instant;

import org.neo4j.common.Subject;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class FailedJobRun
{
    private final long jobId;
    private final Group group;
    private final Subject submitter;
    private final String targetDatabaseName;
    private final String description;
    private final JobType jobType;
    private final Instant submitted;
    private final Instant executionStart;
    private final Instant failureTime;
    private final String failureDescription;

    public FailedJobRun( long jobId, Group group, Subject submitter, String targetDatabaseName, String description, JobType jobType, Instant submitted,
            Instant executionStart, Instant failureTime, Throwable failure )
    {
        this.jobId = jobId;
        this.group = group;
        this.submitter = submitter;
        this.targetDatabaseName = targetDatabaseName;
        this.description = description;
        this.jobType = jobType;
        this.submitted = submitted;
        this.executionStart = executionStart;
        this.failureTime = failureTime;
        this.failureDescription = constructFailureDescription( failure );
    }

    private String constructFailureDescription( Throwable failure )
    {
        String exceptionClass = failure.getClass().getSimpleName();
        String message = failure.getMessage();
        return isNotEmpty( message ) ? exceptionClass + ": " + message : exceptionClass;
    }

    public long getJobId()
    {
        return jobId;
    }

    public Group getGroup()
    {
        return group;
    }

    public Subject getSubmitter()
    {
        return submitter;
    }

    public String getTargetDatabaseName()
    {
        return targetDatabaseName;
    }

    public String getDescription()
    {
        return description;
    }

    public JobType getJobType()
    {
        return jobType;
    }

    public Instant getSubmitted()
    {
        return submitted;
    }

    public Instant getExecutionStart()
    {
        return executionStart;
    }

    public Instant getFailureTime()
    {
        return failureTime;
    }

    public String getFailureDescription()
    {
        return failureDescription;
    }
}
