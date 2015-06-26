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
package org.neo4j.server.preflight;

import java.io.File;
import java.util.Map;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.storemigration.CurrentDatabase;
import org.neo4j.kernel.impl.storemigration.StoreMigrationTool;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.Monitor;
import org.neo4j.kernel.impl.storemigration.StoreVersionCheck;
import org.neo4j.kernel.impl.storemigration.UpgradableDatabase;
import org.neo4j.kernel.impl.storemigration.UpgradeNotAllowedByConfigurationException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.web.ServerInternalSettings;

public class PerformUpgradeIfNecessary implements PreflightTask
{
    private final LogProvider logProvider;
    private String failureMessage = "Unable to upgrade database";
    private final Config config;
    private final Map<String, String> dbConfig;
    private final Log log;
    private final Monitor monitor;

    public PerformUpgradeIfNecessary( Config serverConfig, Map<String, String> dbConfig,
            LogProvider logProvider, StoreUpgrader.Monitor monitor )
    {
        this.config = serverConfig;
        this.dbConfig = dbConfig;
        this.monitor = monitor;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public boolean run()
    {
        try
        {
            File storeDir = config.get( ServerInternalSettings.legacy_db_location );

            FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
            try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( fileSystem, config ) )
            {
                StoreVersionCheck storeVersionCheck = new StoreVersionCheck(
                        pageCache );
                if ( new CurrentDatabase( storeVersionCheck ).storeFilesAtCurrentVersion( storeDir ) )
                {
                    return true;
                }

                UpgradableDatabase upgradableDatabase = new UpgradableDatabase( storeVersionCheck );
                if ( !upgradableDatabase.storeFilesUpgradeable( storeDir ) )
                {
                    return true;
                }

                try
                {
                    new StoreMigrationTool().run( fileSystem, storeDir,
                            new Config( dbConfig ), logProvider, monitor );
                }
                catch ( UpgradeNotAllowedByConfigurationException e )
                {
                    log.error( e.getMessage() );
                    failureMessage = e.getMessage();
                    return false;
                }
                catch ( StoreUpgrader.UnableToUpgradeException e )
                {
                    log.error( "Unable to upgrade store", e );
                    return false;
                }
            }
            return true;
        }
        catch ( Exception e )
        {
            log.error( "Unknown error", e );
            return false;
        }
    }

    @Override
    public String getFailureMessage()
    {
        return failureMessage;
    }
}
