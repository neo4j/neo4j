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
                }
    }

    boolean allowsReads();
    boolean allowsWrites();
    boolean allowsSchemaWrites();
    String name();
}
