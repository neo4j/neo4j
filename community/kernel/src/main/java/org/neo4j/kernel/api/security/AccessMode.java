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

import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.exceptions.Status;

/** Controls the capabilities of a KernelTransaction. */
public interface AccessMode
{
    enum Static implements AccessMode
    {
        /** No reading or writing allowed. */
        NONE
                {
                    @Override
                    public boolean allowsReads()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsWrites()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsSchemaWrites()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsProcedureWith( String[] allowed )
                    {
                        return false;
                    }
                },

        /** No reading or writing allowed because of expired credentials. */
        CREDENTIALS_EXPIRED
                {
                    @Override
                    public boolean allowsReads()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsWrites()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsSchemaWrites()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsProcedureWith( String[] allowed )
                    {
                        return false;
                    }

                    @Override
                    public AuthorizationViolationException onViolation( String msg )
                    {
                        return new AuthorizationViolationException( String.format(
                                msg + "%n%nThe credentials you provided were valid, but must be " +
                                "changed before you can " +
                                "use this instance. If this is the first time you are using Neo4j, this is to " +
                                "ensure you are not using the default credentials in production. If you are not " +
                                "using default credentials, you are getting this message because an administrator " +
                                "requires a password change.%n" +
                                "Changing your password is easy to do via the Neo4j Browser.%n" +
                                "If you are connecting via a shell or programmatically via a driver, " +
                                "just issue a `CALL dbms.changePassword('new password')` statement in the current " +
                                "session, and then restart your driver with the new password configured." ),
                                Status.Security.CredentialsExpired );
                    }
                },

        /** Allows reading data and schema, but not writing. */
        READ
                {
                    @Override
                    public boolean allowsReads()
                    {
                        return true;
                    }

                    @Override
                    public boolean allowsWrites()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsSchemaWrites()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsProcedureWith( String[] allowed )
                    {
                        return false;
                    }
                },

        /** Allows writing data */
        WRITE_ONLY
                {
                    @Override
                    public boolean allowsReads()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsWrites()
                    {
                        return true;
                    }

                    @Override
                    public boolean allowsSchemaWrites()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsProcedureWith( String[] allowed )
                    {
                        return false;
                    }
                },

        /** Allows reading and writing data, but not schema. */
        WRITE
                {
                    @Override
                    public boolean allowsReads()
                    {
                        return true;
                    }

                    @Override
                    public boolean allowsWrites()
                    {
                        return true;
                    }

                    @Override
                    public boolean allowsSchemaWrites()
                    {
                        return false;
                    }

                    @Override
                    public boolean allowsProcedureWith( String[] allowed )
                    {
                        return false;
                    }
                },

        /** Allows all operations. */
        FULL
                {
                    @Override
                    public boolean allowsReads()
                    {
                        return true;
                    }

                    @Override
                    public boolean allowsWrites()
                    {
                        return true;
                    }

                    @Override
                    public boolean allowsSchemaWrites()
                    {
                        return true;
                    }

                    @Override
                    public boolean allowsProcedureWith( String[] allowed )
                    {
                        return true;
                    }
                };

        @Override
        public AuthorizationViolationException onViolation( String msg )
        {
            return new AuthorizationViolationException( msg );
        }
    }

    boolean allowsReads();
    boolean allowsWrites();
    boolean allowsSchemaWrites();

    /**
     * Determines whether this mode allows execution of a procedure with the parameter string array in its
     * procedure annotation.
     *
     * @param allowed An array of strings that encodes permissions that allows the execution of a procedure
     * @return <tt>true</tt> if this mode allows the execution of a procedure with the given parameter string array
     * encoding permission
     * @throws InvalidArgumentsException
     */
    boolean allowsProcedureWith( String[] allowed ) throws InvalidArgumentsException;

    AuthorizationViolationException onViolation( String msg );
    String name();

    default boolean isOverridden()
    {
        return false;
    }
}
