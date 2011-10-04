/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.storemigration.legacystore.LegacyStore;
import org.neo4j.kernel.impl.util.FileUtils;

public class StoreUpgrader
{
    private Map<?, ?> originalConfig;
    private UpgradeConfiguration upgradeConfiguration;
    private UpgradableDatabase upgradableDatabase;

    public StoreUpgrader( Map<?, ?> originalConfig, UpgradeConfiguration upgradeConfiguration, UpgradableDatabase upgradableDatabase )
    {
        this.originalConfig = originalConfig;
        this.upgradeConfiguration = upgradeConfiguration;
        this.upgradableDatabase = upgradableDatabase;
    }

    public void attemptUpgrade( String storageFileName )
    {
        upgradeConfiguration.checkConfigurationAllowsAutomaticUpgrade();
        upgradableDatabase.checkUpgradeable( new File( storageFileName ) );
        try
        {
            File workingDirectory = new File( storageFileName ).getParentFile();

            File upgradeDirectory = new File( workingDirectory, "upgrade" );
            File backupDirectory = new File( workingDirectory, "upgrade_backup" );
            upgradeDirectory.mkdir();

            String upgradeFileName = new File( upgradeDirectory, NeoStore.DEFAULT_NAME ).getPath();
            Map<Object, Object> upgradeConfig = new HashMap<Object, Object>( originalConfig );
            upgradeConfig.put( "neo_store", upgradeFileName );

            NeoStore.createStore( upgradeFileName, upgradeConfig );
            NeoStore neoStore = new NeoStore( upgradeConfig );
            new StoreMigrator( new LegacyStore( storageFileName ) ).migrateTo( neoStore );
            neoStore.close();

            backupDirectory.mkdir();
            StoreFiles.move( workingDirectory, backupDirectory );
            LogFiles.move( workingDirectory, backupDirectory );
            /*
             * Keep an intact copy of the messages log, but let the new db
             * append there.
             */
            FileUtils.copyFile( new File( workingDirectory, "messages.log" ),
                    new File( backupDirectory, "messages.log" ) );

            StoreFiles.move( upgradeDirectory, workingDirectory );
            LogFiles.move( upgradeDirectory, workingDirectory );

        } catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    public static class UnableToUpgradeException extends RuntimeException
    {
    }
}
