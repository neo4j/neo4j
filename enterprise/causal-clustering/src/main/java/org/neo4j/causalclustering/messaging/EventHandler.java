/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.messaging;

import java.util.Objects;

public interface EventHandler
{
    EventHandler EmptyEventHandler = ( eventState, message, throwable, params ) ->
    {
        // do nothing
    };

    default void on( EventState eventState, Param... params )
    {
        on( eventState, "", params );
    }

    default void on( EventState eventState, String message, Param... params )
    {
        on( eventState, message, null, params );
    }

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
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Param param1 = (Param) o;
            return Objects.equals( description, param1.description ) && Objects.equals( param, param1.param );
        }

        @Override
        public int hashCode()
        {

            return Objects.hash( description, param );
        }

        @Override
        public String toString()
        {
            return description + ": " + param;
        }
    }
}
