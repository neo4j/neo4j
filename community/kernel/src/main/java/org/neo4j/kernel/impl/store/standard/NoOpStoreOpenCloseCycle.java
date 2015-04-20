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
package org.neo4j.kernel.impl.store.standard;

import java.io.IOException;

import org.neo4j.io.fs.StoreChannel;

/**
 * A temporary wrapper that disables the open/close logic for a store. This is a shim to allow the new stores to
 * work with the old stores during a transitioning period. The old stores remain responsble for recovery and startup/
 * shutdown logic, the new stores are available for reading and writing in a way that can be mixed with the old stores.
 */
public class NoOpStoreOpenCloseCycle extends StoreOpenCloseCycle
{
    public NoOpStoreOpenCloseCycle()
    {
        super( null, null, null, null );
    }

    @Override
    public void closeStore( StoreChannel channel, long highestIdInUse ) throws IOException
    {

    }

    @Override
    public boolean openStore( StoreChannel channel ) throws IOException
    {
        return false;
    }
}
