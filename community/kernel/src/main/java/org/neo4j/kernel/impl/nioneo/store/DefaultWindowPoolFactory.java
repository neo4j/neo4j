/**
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.graphdb.config.Setting;
import org.neo4j.helpers.Settings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPool;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.helpers.Settings.setting;

public class DefaultWindowPoolFactory implements WindowPoolFactory
{
    @Override
    public WindowPool create( File storageFileName, int recordSize, StoreChannel fileChannel, Config configuration,
                              StringLogger log )
    {

        return new PersistenceWindowPool( storageFileName, recordSize, fileChannel,
                calculateMappedMemory( configuration, storageFileName ),
                configuration.get( CommonAbstractStore.Configuration.use_memory_mapped_buffers ),
                isReadOnly( configuration ) && !isBackupSlave( configuration ),
                new ConcurrentHashMap<Long, PersistenceRow>(), BrickElementFactory.DEFAULT, log );
    }

    private boolean isBackupSlave( Config configuration )
    {
        return configuration.get( CommonAbstractStore.Configuration.backup_slave );
    }

    private boolean isReadOnly( Config configuration )
    {
        return configuration.get( CommonAbstractStore.Configuration.read_only );
    }

    /**
     * Returns memory assigned for
     * {@link MappedPersistenceWindow memory mapped windows} in bytes. The
     * configuration map passed in one constructor is checked for an entry with
     * this stores name.
     *
     * @param config          Map of configuration parameters
     * @param storageFileName Name of the file on disk
     * @return The number of bytes memory mapped windows this store has
     */
    private long calculateMappedMemory( Config config, File storageFileName )
    {
        Long mem = config.get( memoryMappingSetting( storageFileName.getName() ) );
        if ( mem == null )
        {
            mem = 0L;
        }

        return mem;
    }

    public static Setting<Long> memoryMappingSetting( String fileName )
    {
        return setting( fileName + ".mapped_memory", Settings.BYTES, Settings.NO_DEFAULT );
    }
}
