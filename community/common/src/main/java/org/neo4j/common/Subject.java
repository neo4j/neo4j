/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.common;

/**
 * A representation of originator of actions in the database.
 * This is currently used only for monitoring purposes
 * to be able to associate a unit of work with its originator.
 */
public class Subject
{
    /**
     * Used for actions which are not triggered by end users.
     * Typically background maintenance work triggered by the DBMS itself.
     */
    public static final Subject SYSTEM = new Subject( null )
    {
        public String getUsername()
        {
            throw new IllegalStateException( "Getting a username is not supported for System subject" );
        }

        @Override
        public String toString()
        {
            return "SYSTEM";
        }

        public String describe()
        {
            return "";
        }
    };

    /**
     * A representation of an end user when authentication is disabled.
     * This representation is also used when an interface that does not require authentication (typically embedded API) is used.
     */
    public static final Subject AUTH_DISABLED = new Subject( null )
    {
        public String getUsername()
        {
            throw new IllegalStateException( "Getting a username is not supported when authentication is disabled" );
        }

        @Override
        public String toString()
        {
            return "AUTH_DISABLED";
        }

        public String describe()
        {
            return "";
        }
    };

    public static final Subject ANONYMOUS = new Subject( null )
    {
        public String getUsername()
        {
            throw new IllegalStateException( "Getting a username is not supported for anonymous subject" );
        }

        @Override
        public String toString()
        {
            return "ANONYMOUS";
        }

        public String describe()
        {
            return "";
        }
    };

    private final String username;

    public Subject( String username )
    {
        this.username = username;
    }

    public String getUsername()
    {
        return username;
    }

    /**
     * A user-facing description of the subject.
     * <p>
     * The representation returned from this method will be used
     * when displaying the subject for instance
     * in the result of administration commands and functions.
     */
    public String describe()
    {
        return username;
    }
}
