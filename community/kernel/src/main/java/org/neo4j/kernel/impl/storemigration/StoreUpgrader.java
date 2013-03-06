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
package org.neo4j.kernel.impl.storemigration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Uses {@link StoreMigrator} to upgrade automatically when starting up a database, given the right source database,
 * environment and configuration for doing so. The migration will happen to a separate, isolated directory
 * so that an incomplete migration will not affect the original database. Only when a successful migration
 * has taken place the migrated store will replace the original database.
 * 
 * @see StoreMigrator
 */
public class StoreUpgrader
{
    private final Config originalConfig;
    private final UpgradeConfiguration upgradeConfiguration;
    private final UpgradableDatabase upgradableDatabase;
    private final StoreMigrator storeMigrator;
    private final DatabaseFiles databaseFiles;
    private final StringLogger msgLog;
    private final IdGeneratorFactory idGeneratorFactory;
    private final FileSystemAbstraction fileSystemAbstraction;

    public StoreUpgrader( Config originalConfig, StringLogger msgLog, UpgradeConfiguration upgradeConfiguration,
                          UpgradableDatabase upgradableDatabase, StoreMigrator storeMigrator,
                          DatabaseFiles databaseFiles, IdGeneratorFactory idGeneratorFactory,
                          FileSystemAbstraction fileSystemAbstraction )
    {
        this.msgLog = msgLog;
        this.idGeneratorFactory = idGeneratorFactory;
        this.fileSystemAbstraction = fileSystemAbstraction;
        this.originalConfig = originalConfig;
        this.upgradeConfiguration = upgradeConfiguration;
        this.upgradableDatabase = upgradableDatabase;
        this.storeMigrator = storeMigrator;
        this.databaseFiles = databaseFiles;
    }

    public void attemptUpgrade( File storageFileName )
    {
        upgradeConfiguration.checkConfigurationAllowsAutomaticUpgrade();
        upgradableDatabase.checkUpgradeable( storageFileName );

        File workingDirectory = storageFileName.getParentFile();
        File upgradeDirectory = new File( workingDirectory, "upgrade" );
        File backupDirectory = new File( workingDirectory, "upgrade_backup" );

        migrateToIsolatedDirectory( storageFileName, upgradeDirectory );

        databaseFiles.moveToBackupDirectory( workingDirectory, backupDirectory );
        backupMessagesLogLeavingInPlaceForNewDatabaseMessages( workingDirectory, backupDirectory );
        databaseFiles.moveToWorkingDirectory( upgradeDirectory, workingDirectory );
    }

    private void backupMessagesLogLeavingInPlaceForNewDatabaseMessages( File workingDirectory, File backupDirectory )
    {
        try
        {
            fileSystemAbstraction.copyFile( new File( workingDirectory, StringLogger.DEFAULT_NAME ),
                    new File( backupDirectory, StringLogger.DEFAULT_NAME ) );
        }
        catch ( IOException e )
        {
            throw new UnableToUpgradeException( e );
        }
    }

    private void migrateToIsolatedDirectory( File storageFileName, File upgradeDirectory )
    {
        if (upgradeDirectory.exists()) {
            try
            {
                FileUtils.deleteRecursively( upgradeDirectory );
            }
            catch ( IOException e )
            {
                throw new UnableToUpgradeException( e );
            }
        }
        upgradeDirectory.mkdir();

        File upgradeFileName = new File( upgradeDirectory, NeoStore.DEFAULT_NAME );
        Map<String, String> upgradeConfig = new HashMap<String, String>( originalConfig.getParams() );
        upgradeConfig.put( "neo_store", upgradeFileName.getPath() );


        Config upgradeConfiguration = new Config( upgradeConfig );
        
        NeoStore neoStore = new StoreFactory( upgradeConfiguration, idGeneratorFactory, new DefaultWindowPoolFactory(),
                fileSystemAbstraction, StringLogger.DEV_NULL, null ).createNeoStore( upgradeFileName );
        try
        {
            storeMigrator.migrate( new LegacyStore( fileSystemAbstraction, storageFileName, StringLogger.DEV_NULL ),
                    neoStore );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            throw new UnableToUpgradeException( e );
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw Exceptions.launderedException( e );
        }
        finally
        {
            neoStore.close();
        }
    }

    public static class UnableToUpgradeException extends RuntimeException
    {
        public UnableToUpgradeException( Exception cause )
        {
            super( cause );
        }

        public UnableToUpgradeException( String message )
        {
            super( message );
        }
    }
}
