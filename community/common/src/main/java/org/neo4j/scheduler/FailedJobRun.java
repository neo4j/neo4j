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

public class FailedJobRun
{
    private final Group group;
    private final String submitter;
    private final String targetDatabaseName;
    private final String description;
    private final JobType jobType;
    private final Instant submitted;
    private final Instant executionStart;
    private final Instant failureTime;
    private final String failureDescription;

    public FailedJobRun( Group group, String submitter, String targetDatabaseName, String description, JobType jobType, Instant submitted,
            Instant executionStart, Instant failureTime, Throwable failure )
    {
        this.group = group;
        this.submitter = submitter;
        this.targetDatabaseName = targetDatabaseName;
        this.description = description;
        this.jobType = jobType;
        this.submitted = submitted;
        this.executionStart = executionStart;
        this.failureTime = failureTime;

        String s = failure.getClass().getSimpleName();
        String message = failure.getLocalizedMessage();
        this.failureDescription = (message != null) ? (s + ": " + message) : s;
    }

    public Group getGroup()
    {
        return group;
    }

    public String getSubmitter()
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
