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
package org.neo4j.kernel.impl.storageengine;

import org.junit.Before;

import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.test.impl.EphemeralIdGenerator;

public abstract class StorageEngineTest
{
    private IdGeneratorFactory idGeneratorFactory;

    @Before
    public void setUp()
    {
        idGeneratorFactory = new EphemeralIdGenerator.Factory();
    }

    protected abstract StorageEngine buildEngine( IdGeneratorFactory idGeneratorFactory );


    // bigger things
    // TODO provide access to StoreReadLayer
    // TODO create a stream of commands from a transaction state
    // TODO apply a stream of commands to the store
    // TODO deserialise a stream of byte buffers into commands
    // TODO flush to durable storage
}
