/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.event;

/**
 * This typesafe enum [Bloch02] represents an event in Neo.
 */
public class Event
{
    private final String name;

    // Test event for junit tests.
    public static final Event TEST_EVENT = new Event( "TEST_EVENT" );

    // Lifecycle events
    public static final Event NEO_SHUTDOWN_REQUEST = new Event(
        "NEO_SHUTDOWN_REQUEST" );
    public static final Event NEO_SHUTDOWN_STARTED = new Event(
        "NEO_SHUTDOWN_STARTED" ); // only sent proactively
    public static final Event NEO_STARTUP_COMPLETED = new Event(
        "NEO_STARTUP_COMPLETED" );
    public static final Event NEO_FREEZE_REQUEST = new Event(
        "NEO_FREEZE_REQUEST" );
    public static final Event NEO_THAW_REQUEST = new Event( 
        "NEO_THAW_REQUEST" );

    // Neo related events
    public static final Event NODE_CREATE = new Event( "NODE_CREATE" );
    public static final Event NODE_DELETE = new Event( "NODE_DELETE" );
    public static final Event NODE_ADD_PROPERTY = new Event(
        "NODE_ADD_PROPERTY" );
    public static final Event NODE_REMOVE_PROPERTY = new Event(
        "NODE_REMOVE_PROPERTY" );
    public static final Event NODE_CHANGE_PROPERTY = new Event(
        "NODE_CHANGE_PROPERTY" );
    public static final Event NODE_GET_PROPERTY = new Event(
        "NODE_GET_PROPERTY" );
    public static final Event RELATIONSHIP_CREATE = new Event(
        "RELATIONSHIP_CREATE" );
    public static final Event RELATIONSHIP_DELETE = new Event(
        "RELATIONSHIP_DELETE" );
    public static final Event RELATIONSHIP_ADD_PROPERTY = new Event(
        "RELATIONSHIP_ADD_PROPERTY" );
    public static final Event RELATIONSHIP_REMOVE_PROPERTY = new Event(
        "RELATIONSHIP_REMOVE_PROPERTY" );
    public static final Event RELATIONSHIP_CHANGE_PROPERTY = new Event(
        "RELATIONSHIP_CHANGE_PROPERTY" );
    public static final Event RELATIONSHIP_GET_PROPERTY = new Event(
        "RELATIONSHIP_GET_PROPERTY" );
    public static final Event RELATIONSHIPTYPE_CREATE = new Event(
        "RELATIONSHIPTYPE_CREATE" );
    public static final Event PROPERTY_INDEX_CREATE = new Event(
        "PROPERTY_INDEX_CREATE" );

    protected Event( String name )
    {
        this.name = name;
    }

    /**
     * To string method.
     * 
     * @return a string representation of this object
     */
    @Override
    public String toString()
    {
        return name;
    }
}