/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.causalclustering.messaging;

public interface EventHandler
{
    EventHandler EmptyEventHandler = new EventHandler()
    {
        @Override
        public void on( EventState eventState, Param... params )
        {
            // do nothing
        }

        @Override
        public void on( EventState eventState, String message, Param... params )
        {
            // do nothing
        }

        @Override
        public void on( EventState eventState, String message, Throwable throwable, Param... params )
        {
            // do nothing
        }
    };

    void on( EventState eventState, Param... params );

    void on( EventState eventState, String message, Param... params );

    void on( EventState eventState, String message, Throwable throwable, Param... params );

    enum EventState
    {
        Begin,
        Info,
        Error,
        Warn,
        End
    }

    class Param
    {
        private final String description;
        private final Object param;

        public static Param param( String description, Object param )
        {
            return new Param( description, param );
        }

        private Param( String description, Object param )
        {
            this.description = description;
            this.param = param;
        }

        @Override
        public String toString()
        {
            return description + ": " + param;
        }
    }
}
