/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.store;

public interface StoreVersionMismatchHandler
{
    void mismatch( String expected, String found );

    /**
     * @param currentVersionTrailer the version trailer that would be the version of the current store format.
     * @param readVersionTrailer the version previously read when opening the store.
     * @return which version trailer to write to the end of store files at shut down.
     */
    String trailerToWrite( String currentVersionTrailer, String readVersionTrailer );

    public static final StoreVersionMismatchHandler FORCE_CURRENT_VERSION = new StoreVersionMismatchHandler()
    {
        @Override
        public void mismatch( String expected, String found )
        {
            throw new NotCurrentStoreVersionException( expected, found, "", false );
        }

        @Override
        public String trailerToWrite( String currentVersionTrailer, String readVersionTrailer )
        {
            return currentVersionTrailer;
        }
    };

    public static final StoreVersionMismatchHandler ALLOW_OLD_VERSION = new StoreVersionMismatchHandler()
    {
        @Override
        public void mismatch( String expected, String found )
        {   // Dangerous, but used for dependency satisfaction during store migration
        }

        @Override
        public String trailerToWrite( String currentVersionTrailer, String readVersionTrailer )
        {
            return readVersionTrailer;
        }
    };
}
