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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

public class StoreUpgrader
{
    private Config originalConfig;
    private UpgradeConfiguration upgradeConfiguration;
    private UpgradableDatabase upgradableDatabase;
    private StoreMigrator storeMigrator;
    private DatabaseFiles databaseFiles;
    private StringLogger msgLog;
    private IdGeneratorFactory idGeneratorFactory;
    private FileSystemAbstraction fileSystemAbstraction;

    public StoreUpgrader( Config originalConfig, StringLogger msgLog, UpgradeConfiguration upgradeConfiguration, UpgradableDatabase upgradableDatabase,
                          StoreMigrator storeMigrator, DatabaseFiles databaseFiles,
                          IdGeneratorFactory idGeneratorFactory, FileSystemAbstraction fileSystemAbstraction)
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

    public void attemptUpgrade( String storageFileName )
    {
        upgradeConfiguration.checkConfigurationAllowsAutomaticUpgrade();
        upgradableDatabase.checkUpgradeable( new File( storageFileName ) );

        File workingDirectory = new File( storageFileName ).getParentFile();
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
            FileUtils.copyFile( new File( workingDirectory, "messages.log" ),
                    new File( backupDirectory, "messages.log" ) );
        }
        catch ( IOException e )
        {
            throw new UnableToUpgradeException( e );
        }
    }

    private void migrateToIsolatedDirectory( String storageFileName, File upgradeDirectory )
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

        String upgradeFileName = new File( upgradeDirectory, NeoStore.DEFAULT_NAME ).getPath();
        Map<String, String> upgradeConfig = new HashMap<String, String>( originalConfig.getParams() );
        upgradeConfig.put( "neo_store", upgradeFileName );
        
        Config upgradeConfiguration = new Config( msgLog, fileSystemAbstraction, upgradeConfig, Collections.<Class<?>>singletonList( GraphDatabaseSettings.class ) );
        
        NeoStore neoStore = new StoreFactory(upgradeConfiguration, idGeneratorFactory, fileSystemAbstraction, null, StringLogger.DEV_NULL, null).createNeoStore(upgradeFileName);
        try
        {
            storeMigrator.migrate( new LegacyStore( storageFileName ), neoStore );
        }
        catch ( IOException e )
        {
            throw new UnableToUpgradeException( e );
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
