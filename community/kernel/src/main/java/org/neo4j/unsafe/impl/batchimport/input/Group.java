/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;

/**
 * Group of {@link InputEntity inputs}. Used primarily in {@link IdMapper} for supporting multiple
 * id groups within the same index.
 */
public interface Group
{
    int id();

    public static class Adapter implements Group
    {
        private final int id;
        private final String name;

        public Adapter( int id, String name )
        {
            this.id = id;
            this.name = name;
        }

        @Override
        public int id()
        {
            return id;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    public static final Group GLOBAL = new Adapter( 0, "global" );
}
