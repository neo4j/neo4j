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
package org.neo4j.graphdb.event;

/**
 * An object that describes a state from which a Neo4j Graph Database cannot
 * continue.
 *
 * @author Tobias Ivarsson
 */
public enum ErrorState
{
    /**
     * The Graph Database failed since the storage media where the graph
     * database data is stored is full and cannot be written to.
     */
    STORAGE_MEDIA_FULL,
    
    /**
     * Not more transactions can be started or committed during this session
     * and the database needs to be shut down, possible for maintenance before
     * it can be started again.
     */
    TX_MANAGER_NOT_OK,
}
