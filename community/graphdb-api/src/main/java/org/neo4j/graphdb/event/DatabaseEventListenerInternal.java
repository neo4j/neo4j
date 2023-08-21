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

/**
 * FIXME ODP
 * This class is here to hide {@link #databaseOutOfDiskSpace(DatabaseEventContext)} from public API during development.
 * Pull members up to super interface when done.
 */
public interface DatabaseEventListenerInternal extends DatabaseEventListener {
    /**
     * This method is invoked when a database component cannot allocate as much disk space as it needs to complete some necessary operation,
     * such as growing store files, tx-log or index files.
     * @param event context of the event, can be used to get metadata.
     */
    void databaseOutOfDiskSpace(DatabaseEventContext event);
}
