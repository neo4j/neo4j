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

import java.util.Objects;

import org.neo4j.common.Subject;

import static org.neo4j.common.Subject.AUTH_DISABLED;

public class JobMonitoringParams
{
    public static final JobMonitoringParams NOT_MONITORED = new JobMonitoringParams( null, null, null );

    private final Subject submitter;
    private final String targetDatabaseName;
    private final String description;

    public JobMonitoringParams( Subject submitter, String targetDatabaseName, String description )
    {
        this.submitter = Objects.requireNonNullElse( submitter, AUTH_DISABLED );
        this.targetDatabaseName = targetDatabaseName;
        this.description = description;
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

    @Override
    public String toString()
    {
        return "JobMonitoringParams{" +
                "submitter=" + submitter +
                ", targetDatabaseName='" + targetDatabaseName + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
