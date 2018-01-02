/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.graphdb;

/**
 * Representation for notifications found when executing a query.
 *
 * A notification can be visualized in a client pinpointing problems or other information about the query.
 */
public interface Notification
{
    /**
     * Returns a notification code for the discovered issue.
     * @return the notification code
     */
    String getCode();

    /**
     * Returns a short summary of the notification.
     * @return the title of the notification.
     */
    String getTitle();

    /**
     * Returns a longer description of the notification.
     * @return the description of the notification.
     */
    String getDescription();

    /**
     * Returns the severity level of this notification.
     * @return the severity level of the notification.
     */
    SeverityLevel getSeverity();

    /**
     * The position in the query where this notification points to.
     * Not all notifications have a unique position to point to and should in
     * that case return {@link org.neo4j.graphdb.InputPosition#empty}
     *
     * @return the position in the query where the issue was found, or
     * {@link org.neo4j.graphdb.InputPosition#empty} if no position is associated with this notification.
     */
    InputPosition getPosition();
}
