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
package org.neo4j.kernel.impl.store.kvstore;

import java.io.File;

public interface RotationMonitor
{
    void failedToOpenStoreFile( File path, Exception failure );

    void beforeRotation( File from, File next, Headers headers );

    void rotationFailed( File from, File next, Headers headers, Exception failure );

    void rotationSucceeded( File from, File next, Headers headers );

    RotationMonitor NONE = new RotationMonitor()
    {
        @Override
        public void failedToOpenStoreFile( File path, Exception failure )
        {
        }

        @Override
        public void beforeRotation( File from, File next, Headers headers )
        {
        }

        @Override
        public void rotationFailed( File from, File next, Headers headers, Exception failure )
        {
        }

        @Override
        public void rotationSucceeded( File from, File next, Headers headers )
        {
        }
    };
}
