/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.logging.Logger;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPool;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

public class DefaultWindowPoolFactory implements WindowPoolFactory
{
    @Deprecated
    private static Logger logger = Logger.getLogger( DefaultWindowPoolFactory.class.getName() );

    @Override
    public WindowPool create( String storageFileName, int recordSize, FileChannel fileChannel, Config configuration,
                              StringLogger log )
    {

        return new PersistenceWindowPool( storageFileName, recordSize, fileChannel,
                calculateMappedMemory( configuration.getParams(), storageFileName ),
                configuration.get( CommonAbstractStore.Configuration.use_memory_mapped_buffers ),
                isReadOnly( configuration ) && !isBackupSlave( configuration ), log );
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
     * @param log
     * @return The number of bytes memory mapped windows this store has
     */
    // TODO: This should use the type-safe config API, rather than this magic stuff
    private long calculateMappedMemory( Map<?, ?> config, String storageFileName )
    {
        String convertSlash = storageFileName.replace( '\\', '/' );
        String realName = convertSlash.substring( convertSlash
                .lastIndexOf( '/' ) + 1 );
        String mem = (String) config.get( realName + ".mapped_memory" );
        if ( mem != null )
        {
            long multiplier = 1;
            mem = mem.trim().toLowerCase();
            if ( mem.endsWith( "m" ) )
            {
                multiplier = 1024 * 1024;
                mem = mem.substring( 0, mem.length() - 1 );
            }
            else if ( mem.endsWith( "k" ) )
            {
                multiplier = 1024;
                mem = mem.substring( 0, mem.length() - 1 );
            }
            else if ( mem.endsWith( "g" ) )
            {
                multiplier = 1024 * 1024 * 1024;
                mem = mem.substring( 0, mem.length() - 1 );
            }
            try
            {
                return Integer.parseInt( mem ) * multiplier;
            }
            catch ( NumberFormatException e )
            {
                logger.info( "Unable to parse mapped memory[" + mem
                        + "] string for " + storageFileName );
            }
        }

        return 0;
    }

}
