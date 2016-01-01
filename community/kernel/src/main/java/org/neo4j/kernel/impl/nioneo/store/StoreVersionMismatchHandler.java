/**
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
package org.neo4j.kernel.impl.nioneo.store;

public interface StoreVersionMismatchHandler
{
    void mismatch( String expected, String found );

    public static final StoreVersionMismatchHandler THROW_EXCEPTION = new StoreVersionMismatchHandler()
    {
        @Override
        public void mismatch( String expected, String found )
        {
            throw new NotCurrentStoreVersionException( expected, found, "", false );
        }
    };

    public static final StoreVersionMismatchHandler ACCEPT = new StoreVersionMismatchHandler()
    {
        @Override
        public void mismatch( String expected, String found )
        {   // Dangerous, but used for dependency satisfaction during store migration
        }
    };
}
