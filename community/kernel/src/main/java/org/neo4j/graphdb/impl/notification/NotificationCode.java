/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.graphdb.impl.notification;


import org.neo4j.graphdb.InputPosition;
import org.neo4j.graphdb.SeverityLevel;
import org.neo4j.kernel.api.exceptions.Status;

/**
 * Notification codes are status codes identifying the type of notification.
 */
public enum NotificationCode
{
    CARTESIAN_PRODUCT( SeverityLevel.WARNING,
           Status.Statement.CartesianProduct,
            "If a part of a query contains multiple disconnected patterns, this will build a " +
                    "cartesian product between all those parts. This may produce a large amount of data and slow down" +
                    " query processing. " +
                    "While occasionally intended, it may often be possible to reformulate the query that avoids the " +
                    "use of this cross " +
                    "product, perhaps by adding a relationship between the different parts or by using OPTIONAL MATCH" ),
    LEGACY_PLANNER( SeverityLevel.WARNING,
                    Status.Statement.DeprecationWarning,
                    "Using PLANNER for switching between planners has been deprecated, please use CYPHER planner=[rule,cost] instead");
    private final Status status;
    private final String description;
    private final SeverityLevel severity;
    NotificationCode(SeverityLevel severity, Status status, String description)
    {
        this.severity = severity;
        this.status = status;
        this.description = description;
    }

    public Notification notification( InputPosition position )
    {
        return new Notification( position );
    }

    private final class Notification implements org.neo4j.graphdb.Notification
    {
        private final InputPosition position;

        public Notification( InputPosition position )
        {
            this.position = position;
        }

        public String getCode()
        {
            return status.code().serialize();
        }

        public String getTitle()
        {
            return status.code().description();
        }

        public String getDescription()
        {
            return description;
        }

        public InputPosition getPosition()
        {
            return position;
        }

        public SeverityLevel getSeverity()
        {
            return severity;
        }
    }
}
