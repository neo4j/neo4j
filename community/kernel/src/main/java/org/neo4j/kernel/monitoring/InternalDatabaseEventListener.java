/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.monitoring;

import org.neo4j.graphdb.event.DatabaseEventListener;

/**
 * Event listener providing detailed information about database life cycle events.
 * Equivalent to {@link DatabaseEventListener} but not intended for consumption as part
 * of the public api.
 */
public interface InternalDatabaseEventListener
{
    /**
     * This method is invoked after start of a specific database. Database is completely operational on the moment of notification.
     * @param startDatabaseEvent context of the event, can be used to get metadata.
     */
    void databaseStart( StartDatabaseEvent startDatabaseEvent );

    /**
     * This method is invoked before shutdown process of a specific database. Database is still completely operational on the moment of notification.
     * @param stopDatabaseEvent context of the event, can be used to get metadata.
     */
    void databaseShutdown( StopDatabaseEvent stopDatabaseEvent );

    /**
     * This method is invoked when the particular database enters a state from which it cannot recover and continue.
     * @param panicDatabaseEvent context of the event, can be used to get metadata.
     */
    void databasePanic( PanicDatabaseEvent panicDatabaseEvent );

    /**
     * This method is invoked when a new database is created. This is called before {@link #databaseStart(StartDatabaseEvent)}.
     * @param createDatabaseEvent context of the event, can be used to get metadata.
     */
    void databaseCreate( CreateDatabaseEvent createDatabaseEvent );

    /**
     * This method is invoked after the database is dropped. This is intended to be used when cleaning up database specific files after the database
     * is deleted by the user.
     * @param dropDatabaseEvent context of the event, can be used to get metadata.
     */
    void databaseDrop( DropDatabaseEvent dropDatabaseEvent );
}
