/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.neo4j.kernel.CommonFactories;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.storemigration.monitoring.VisibleMigrationProgressMonitor;

public class StoreMigrationTool
{
    public static void main( String[] args ) throws IOException
    {
        String legacyStoreDirectory = args[0];
        String targetStoreDirectory = args[1];

        new StoreMigrationTool().run( legacyStoreDirectory, targetStoreDirectory );
    }

    private void run( String legacyStoreDirectory, String targetStoreDirectory ) throws IOException
    {
        LegacyStore legacyStore = new LegacyStore( new File( new File( legacyStoreDirectory ), NeoStore.DEFAULT_NAME ).getPath() );

        HashMap config = new HashMap();
        config.put( IdGeneratorFactory.class, CommonFactories.defaultIdGeneratorFactory() );
        config.put( FileSystemAbstraction.class, CommonFactories.defaultFileSystemAbstraction() );

        File targetStoreDirectoryFile = new File( targetStoreDirectory );
        if ( targetStoreDirectoryFile.exists() )
        {
            throw new IllegalStateException( "Cannot migrate to a directory that already exists, please delete first and re-run" );
        }
        boolean success = targetStoreDirectoryFile.mkdirs();
        if ( !success )
        {
            throw new IllegalStateException( "Failed to create directory" );
        }

        File targetStoreFile = new File( targetStoreDirectory, NeoStore.DEFAULT_NAME );
        config.put( "neo_store", targetStoreFile.getPath() );
        NeoStore.createStore( targetStoreFile.getPath(), config );
        NeoStore neoStore = new NeoStore( config );

        long startTime = System.currentTimeMillis();

        new StoreMigrator( new VisibleMigrationProgressMonitor( System.out ) ) .migrate( legacyStore, neoStore );

        long duration = System.currentTimeMillis() - startTime;
        System.out.printf( "Migration completed in %d s%n", duration / 1000 );

        neoStore.close();

        EmbeddedGraphDatabase database = new EmbeddedGraphDatabase( targetStoreDirectoryFile.getPath() );
        database.shutdown();
    }
}
