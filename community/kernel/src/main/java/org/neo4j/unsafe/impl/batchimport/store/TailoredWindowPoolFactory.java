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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPool;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * A {@link WindowPoolFactory} that can assign specific {@link WindowPool window pools} tailored
 * for each individual store.
 */
public class TailoredWindowPoolFactory implements WindowPoolFactory
{
    private final WindowPoolFactory defaultFactory;
    private final Map<String, WindowPoolFactory> overrides = new HashMap<>();

    public TailoredWindowPoolFactory( WindowPoolFactory defaultFactory )
    {
        this.defaultFactory = defaultFactory;
    }

    public void override( String storeNamePart, WindowPoolFactory override )
    {
        overrides.put( NeoStore.DEFAULT_NAME + storeNamePart, override );
    }

    @Override
    public WindowPool create( File storageFileName, int recordSize, StoreChannel fileChannel, Config configuration,
            StringLogger log, int numberOfReservedLowIds )
    {
        WindowPoolFactory override = overrides.get( storageFileName.getName() );
        WindowPoolFactory factory = override != null ? override : defaultFactory;
        return factory.create( storageFileName, recordSize, fileChannel, configuration, log, numberOfReservedLowIds );
    }
}
