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
package org.neo4j.storageengine;

/**
 * Every time we write to transaction logs we write some chunk of data (transaction, part of it, etc) and to be able to find that data later we need to identify it.
 * To ensure the retrievability of this data in the future, it must be identifiable.
 * To facilitate this, each append operation is assigned a distinct append index value, sourced from an index provider, which establishes the sequence of these operations.
 */
public interface AppendIndexProvider {
    long BASE_APPEND_INDEX = 1;

    long UNKNOWN_APPEND_INDEX = BASE_APPEND_INDEX - 1;

    long nextAppendIndex();

    long getLastAppendIndex();
}
