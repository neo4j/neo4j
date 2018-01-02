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

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Event handler interface for Neo4j Kernel life cycle events.
 * 
 * @author Tobias Ivarsson
 */
public interface KernelEventHandler
{
    /**
     * This method is invoked during the shutdown process of a Neo4j Graph
     * Database. It is invoked while the {@link GraphDatabaseService} is still
     * in an operating state, after the processing of this event has terminated
     * the Neo4j Graph Database will terminate. This event can be used to shut
     * down other services that depend on the {@link GraphDatabaseService}.
     */
    void beforeShutdown();

    /**
     * This is invoked when the Neo4j Graph Database enters a state from which
     * it cannot continue.
     *
     * @param error an object describing the state that the
     *            {@link GraphDatabaseService} failed to recover from.
     */
    void kernelPanic( ErrorState error );

    /**
     * Returns the resource associated with this event handler, or {@code null}
     * if no specific resource is associated with this handler or if it isn't
     * desirable to expose it. It can be used to aid in the decision process
     * of in which order to execute the handlers, see
     * {@link #orderComparedTo(KernelEventHandler)}.
     *
     * @return the resource associated to this event handler, or {@code null}.
     */
    Object getResource();

    /**
     * Gives a hint about when to execute this event handler, compared to other
     * handlers. If this handler must be executed before {@code other} then
     * {@link ExecutionOrder#BEFORE} should be returned. If this handler must be
     * executed after {@code other} then {@link ExecutionOrder#AFTER} should be
     * returned. If it doesn't matter {@link ExecutionOrder#DOESNT_MATTER}
     * should be returned.
     *
     * @param other the other event handler to compare to.
     * @return the execution order compared to {@code other}.
     */
    ExecutionOrder orderComparedTo( KernelEventHandler other );

    /**
     * Represents the order of execution between two event handlers, if one
     * handler should be executed {@link ExecutionOrder#BEFORE},
     * {@link ExecutionOrder#AFTER} another handler, or if it
     * {@link ExecutionOrder#DOESNT_MATTER}.
     * 
     * @author mattias
     * 
     */
    enum ExecutionOrder
    {
        /**
         * Says that the event handler must be executed before the compared
         * event handler.
         */
        BEFORE,

        /**
         * Says that the event handler must be executed after the compared
         * event handler.
         */
        AFTER,

        /**
         * Says that it doesn't matter in which order the event handler is
         * executed in comparison to another event handler.
         */
        DOESNT_MATTER
    }
}
