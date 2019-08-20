/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.api.impl.fulltext;

import java.io.File;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.extension.ExtensionFactory;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.context.ExtensionContext;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.recovery.RecoveryExtension;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.token.TokenHolders;

import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.directoryFactory;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;

@RecoveryExtension
@ServiceProvider
public class FulltextIndexProviderFactory extends ExtensionFactory<FulltextIndexProviderFactory.Dependencies>
{
    private static final String KEY = "fulltext";
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( KEY, "1.0" );

    public interface Dependencies
    {
        Config getConfig();

        FileSystemAbstraction fileSystem();

        JobScheduler scheduler();

        TokenHolders tokenHolders();

        GlobalProcedures procedures();

        LogService getLogService();
    }

    public FulltextIndexProviderFactory()
    {
        super( ExtensionType.DATABASE, KEY );
    }

    private static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File storeDir )
    {
        return directoriesByProvider( storeDir );
    }

    @Override
    public Lifecycle newInstance( ExtensionContext context, Dependencies dependencies )
    {
        Config config = dependencies.getConfig();
        boolean ephemeral = config.get( GraphDatabaseSettings.ephemeral );
        FileSystemAbstraction fileSystemAbstraction = dependencies.fileSystem();
        DirectoryFactory directoryFactory = directoryFactory( ephemeral );
        OperationalMode operationalMode = context.databaseInfo().operationalMode;
        JobScheduler scheduler = dependencies.scheduler();
        IndexDirectoryStructure.Factory directoryStructureFactory = subProviderDirectoryStructure( context.directory() );
        TokenHolders tokenHolders = dependencies.tokenHolders();
        Log log = dependencies.getLogService().getInternalLog( FulltextIndexProvider.class );

        return new FulltextIndexProvider(
                DESCRIPTOR, directoryStructureFactory, fileSystemAbstraction, config, tokenHolders,
                directoryFactory, operationalMode, scheduler, log );
    }
}
