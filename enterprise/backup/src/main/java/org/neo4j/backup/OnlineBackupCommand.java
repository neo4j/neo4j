/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup;

import java.nio.file.Path;

import org.neo4j.commandline.admin.AdminCommand;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.IncorrectUsage;
import org.neo4j.commandline.admin.OutsideWorld;

public class OnlineBackupCommand implements AdminCommand
{
    private final OutsideWorld outsideWorld;
    private final OnlineBackupContextLoader onlineBackupContextLoader;
    private final BackupFlowFactory backupFlowFactory;
    private final AbstractBackupSupportingClassesFactory backupSupportingClassesFactory;

    /**
     * The entry point for neo4j admin tool's online backup functionality
     *
     * @param outsideWorld provides a way to interact with the filesystem and output streams
     * @param onlineBackupContextLoader helper class to validate, process and return a grouped result of processing the command line arguments
     * @param backupSupportingClassesFactory necessary for constructing the strategy for backing up over the causal clustering transaction protocol
     * @param backupFlowFactory class that actually handles the logic of performing a backup
     */
    OnlineBackupCommand( OutsideWorld outsideWorld, OnlineBackupContextLoader onlineBackupContextLoader,
            AbstractBackupSupportingClassesFactory backupSupportingClassesFactory, BackupFlowFactory backupFlowFactory )
    {
        this.outsideWorld = outsideWorld;
        this.onlineBackupContextLoader = onlineBackupContextLoader;
        this.backupSupportingClassesFactory = backupSupportingClassesFactory;
        this.backupFlowFactory = backupFlowFactory;
    }

    @Override
    public void execute( String[] args ) throws IncorrectUsage, CommandFailed
    {
        OnlineBackupContext onlineBackupContext = onlineBackupContextLoader.fromCommandLineArguments( args );
        BackupSupportingClasses backupSupportingClasses =
                backupSupportingClassesFactory.createSupportingClassesForBackupStrategies( onlineBackupContext.getConfig() );

        // Make sure destination exists
        checkDestination( onlineBackupContext.getRequiredArguments().getFolder() );
        checkDestination( onlineBackupContext.getRequiredArguments().getReportDir() );

        BackupFlow backupFlow = backupFlowFactory.backupFlow( onlineBackupContext, backupSupportingClasses.getBackupProtocolService(),
                backupSupportingClasses.getBackupDelegator() );

        backupFlow.performBackup( onlineBackupContext );
        outsideWorld.stdOutLine( "Backup complete." );
    }

    private void checkDestination( Path path ) throws CommandFailed
    {
        if ( !outsideWorld.fileSystem().isDirectory( path.toFile() ) )
        {
            throw new CommandFailed( String.format( "Directory '%s' does not exist.", path ) );
        }
    }
}
