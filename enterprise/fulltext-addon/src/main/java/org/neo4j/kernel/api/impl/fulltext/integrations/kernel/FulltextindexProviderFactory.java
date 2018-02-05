/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext.integrations.kernel;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Service;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.PropertyKeyTokenHolder;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

@Service.Implementation( KernelExtensionFactory.class )
public class FulltextindexProviderFactory extends KernelExtensionFactory<FulltextindexProviderFactory.Dependencies>
{
    public static final String KEY = "fulltext";
    public static final IndexProvider.Descriptor DESCRIPTOR = new IndexProvider.Descriptor( KEY, "1.0" );
    private static final int PRIORITY = 0;

    public FulltextindexProviderFactory()
    {
        super( KEY );
    }

    public static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File storeDir )
    {
        IndexDirectoryStructure parentDirectoryStructure = directoriesByProvider( storeDir ).forProvider( DESCRIPTOR );
        return directoriesBySubProvider( parentDirectoryStructure );
    }

    @Override
    public FulltextIndexProvider newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        String analyzer = dependencies.getConfig().get( FulltextConfig.fulltext_default_analyzer );

        return new FulltextIndexProvider( DESCRIPTOR, PRIORITY, subProviderDirectoryStructure( context.storeDir() ), dependencies.fileSystem(), analyzer,
                dependencies.propertyKeyTokenHolder(), dependencies.logService(), dependencies.availabilityGuard(), dependencies.db(), dependencies.scheduler(), dependencies::transactionIdStore, context.storeDir() );
    }

    public interface Dependencies
    {
        Config getConfig();

        GraphDatabaseService db();

        FileSystemAbstraction fileSystem();

        PropertyKeyTokenHolder propertyKeyTokenHolder();

        Procedures procedures();

        LogService logService();

        AvailabilityGuard availabilityGuard();

        JobScheduler scheduler();

        TransactionIdStore transactionIdStore();
    }
}
