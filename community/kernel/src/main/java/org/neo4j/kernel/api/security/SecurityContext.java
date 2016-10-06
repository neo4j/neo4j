/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.security;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

/** Controls the capabilities of a KernelTransaction. */
public interface SecurityContext
{
    enum Static implements SecurityContext
    {
        /** No reading or writing allowed. */
        NONE
                {
                    @Override
                    public Allowance allows()
                    {
                        return Allowance.Static.NONE;
                    }
                },

        /** No reading or writing allowed because of expired credentials. */
        CREDENTIALS_EXPIRED
                {
                    @Override
                    public Allowance allows()
                    {
                        return Allowance.Static.CREDENTIALS_EXPIRED;
                    }
                },

        /** Allows reading data and schema, but not writing. */
        READ
                {
                    @Override
                    public Allowance allows()
                    {
                        return Allowance.Static.READ;
                    }
                },

        /** Allows writing data */
        WRITE_ONLY
                {
                    @Override
                    public Allowance allows()
                    {
                        return Allowance.Static.WRITE_ONLY;
                    }
                },

        /** Allows reading and writing data, but not schema. */
        WRITE
                {
                    @Override
                    public Allowance allows()
                    {
                        return Allowance.Static.WRITE;
                    }
                },

        /** Allows all operations. */
        FULL
                {
                    @Override
                    public Allowance allows()
                    {
                        return Allowance.Static.FULL;
                    }
                }
        }

    Allowance allows();
    String name();

    /**
     * Determines whether this mode allows execution of a procedure with the parameter string array in its
     * procedure annotation.
     *
     * @param allowed An array of strings that encodes permissions that allows the execution of a procedure
     * @return <tt>true</tt> if this mode allows the execution of a procedure with the given parameter string array
     * encoding permission
     * @throws InvalidArgumentsException
     */
    default boolean allowsProcedureWith( String[] allowed ) throws InvalidArgumentsException
    {
        return false;
    }

    default String username()
    {
        return ""; // Should never clash with a valid username
    }

    SecurityContext AUTH_DISABLED = new SecurityContext() {

        @Override
        public Allowance allows()
        {
            return Allowance.Static.FULL;
        }

        @Override
        public String name()
        {
            return "auth-disabled";
        }
    };
}
