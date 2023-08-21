/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.event;

import org.neo4j.annotations.api.PublicApi;

/**
 * Event listener interface for independent database life cycle events.
 * Listeners are invoked on the same thread as event handling itself as result any blocking operation in the listener will block execution of the event.
 */
@PublicApi
public interface DatabaseEventListener {
    /**
     * This method is invoked after start of a specific database. Database is completely operational on the moment of notification.
     * @param eventContext context of the event, can be used to get metadata.
     */
    void databaseStart(DatabaseEventContext eventContext);

    /**
     * This method is invoked before shutdown process of a specific database. Database is still completely operational on the moment of notification.
     * @param eventContext context of the event, can be used to get metadata.
     */
    void databaseShutdown(DatabaseEventContext eventContext);

    /**
     * This method is invoked when the particular database enters a state from which it cannot recover and continue.
     * @param eventContext context of the event, can be used to get metadata.
     */
    void databasePanic(DatabaseEventContext eventContext);

    /**
     * This method is invoked when a new database is created. This is called before {@link #databaseStart(DatabaseEventContext)} and before any database
     * components are available.
     * @param eventContext context of the event, can be used to get metadata.
     */
    void databaseCreate(DatabaseEventContext eventContext);

    /**
     * This method is invoked after the database is dropped. This is intended to be used when cleaning up database specific files after the database
     * is deleted by the user.
     * @param eventContext context of the event, can be used to get metadata.
     */
    void databaseDrop(DatabaseEventContext eventContext);
}
