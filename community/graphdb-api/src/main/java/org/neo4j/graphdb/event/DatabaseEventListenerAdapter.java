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
 * Adapter for event listener interface for database lifecycle events.
 */
public class DatabaseEventListenerAdapter implements DatabaseEventListenerInternal {
    @Override
    public void databaseStart(DatabaseEventContext eventContext) {
        // empty
    }

    @Override
    public void databaseShutdown(DatabaseEventContext eventContext) {
        // empty
    }

    @Override
    public void databasePanic(DatabaseEventContext eventContext) {
        // empty
    }

    @Override
    public void databaseCreate(DatabaseEventContext eventContext) {
        // empty
    }

    @Override
    public void databaseDrop(DatabaseEventContext eventContext) {
        // empty
    }

    @Override
    public void databaseOutOfDiskSpace(DatabaseEventContext event) {
        // empty
    }
}
