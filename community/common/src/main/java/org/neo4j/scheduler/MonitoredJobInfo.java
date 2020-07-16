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

import java.time.Duration;
import java.time.Instant;

import org.neo4j.common.Subject;

public class MonitoredJobInfo
{
    private final long id;
    private final Group group;
    private final Subject submitter;
    private final String targetDatabaseName;
    private final Instant submitted;
    private final String description;
    private final Instant nextDeadline;
    private final Duration period;
    private final State state;
    private final JobType type;
    private final String currentStateDescription;

    public MonitoredJobInfo( long id, Group group, Instant submitted, Subject submitter, String targetDatabaseName, String description,
            Instant nextDeadline, Duration period, State state, JobType type, String currentStateDescription )
    {
        this.id = id;
        this.group = group;
        this.submitter = submitter;
        this.targetDatabaseName = targetDatabaseName;
        this.submitted = submitted;
        this.description = description;
        this.nextDeadline = nextDeadline;
        this.period = period;
        this.state = state;
        this.type = type;
        this.currentStateDescription = currentStateDescription;
    }

    public long getId()
    {
        return id;
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

    public Instant getSubmitted()
    {
        return submitted;
    }

    public String getDescription()
    {
        return description;
    }

    public Instant getNextDeadline()
    {
        return nextDeadline;
    }

    public Duration getPeriod()
    {
        return period;
    }

    public State getState()
    {
        return state;
    }

    public JobType getType()
    {
        return type;
    }

    public String getCurrentStateDescription()
    {
        return currentStateDescription;
    }

    public enum State
    {
        /**
         * Scheduled for an execution at a point in the future.
         * Only {@link JobType#DELAYED} and {@link JobType#PERIODIC} can be in this state.
         */
        SCHEDULED,
        /**
         * Actively executed by an executors thread.
         */
        EXECUTING,
    }
}
