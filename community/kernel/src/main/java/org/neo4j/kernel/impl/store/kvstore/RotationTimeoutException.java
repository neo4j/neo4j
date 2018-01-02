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

import org.neo4j.kernel.impl.store.StoreFailureException;

/**
 * When {@link org.neo4j.kernel.impl.store.kvstore.AbstractKeyValueStore.RotationTask} do rotation without force
 * option specified, it will wait for all transactions below specified version before doing rotation
 * in case if they will not finish for specified timeout rotation will be terminated and exception will be thrown.
 * @see org.neo4j.kernel.impl.store.kvstore.AbstractKeyValueStore.RotationTask
 */
public class RotationTimeoutException extends StoreFailureException
{

    private static final String MESSAGE_TEMPLATE = "Failed to rotate logs. Expected version: %d, actual " +
                                                   "version: %d, wait timeout (ms): %d";

    public RotationTimeoutException( long expectedVersion, long actualVersion, long rotationDuration )
    {
        super( String.format( MESSAGE_TEMPLATE, expectedVersion, actualVersion, rotationDuration ) );
    }
}
