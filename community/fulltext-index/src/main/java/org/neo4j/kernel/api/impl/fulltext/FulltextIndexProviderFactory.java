/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.storage.DirectoryFactory;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.txstate.auxiliary.AuxiliaryTransactionStateManager;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.ExtensionType;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.NonTransactionalTokenNameLookup;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.kernel.api.impl.index.storage.DirectoryFactory.directoryFactory;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesBySubProvider;

@Service.Implementation( KernelExtensionFactory.class )
public class FulltextIndexProviderFactory extends KernelExtensionFactory<FulltextIndexProviderFactory.Dependencies>
{
    private static final String KEY = "fulltext";
    public static final IndexProviderDescriptor DESCRIPTOR = new IndexProviderDescriptor( KEY, "1.0" );

    public interface Dependencies
    {
        Config getConfig();

        FileSystemAbstraction fileSystem();

        JobScheduler scheduler();

        TokenHolders tokenHolders();

        Procedures procedures();

        LogService getLogService();

        AuxiliaryTransactionStateManager auxiliaryTransactionStateManager();
    }

    public FulltextIndexProviderFactory()
    {
        super( ExtensionType.DATABASE, KEY );
    }

    private static IndexDirectoryStructure.Factory subProviderDirectoryStructure( File storeDir )
    {
        IndexDirectoryStructure parentDirectoryStructure = directoriesByProvider( storeDir ).forProvider( DESCRIPTOR );
        return directoriesBySubProvider( parentDirectoryStructure );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies )
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
        AuxiliaryTransactionStateManager auxiliaryTransactionStateManager;
        try
        {
            auxiliaryTransactionStateManager = dependencies.auxiliaryTransactionStateManager();
        }
        catch ( UnsatisfiedDependencyException e )
        {
            String message = "Fulltext indexes failed to register as transaction state providers. This means that, if queried, they will not be able to " +
                    "uncommitted transactional changes into account. This is fine if the indexes are opened for non-transactional work, such as for " +
                    "consistency checking. The reason given is: " + e.getMessage();
            logDependencyException( context, log.errorLogger(), message );
            auxiliaryTransactionStateManager = new NullAuxiliaryTransactionStateManager();
        }

        FulltextIndexProvider provider = new FulltextIndexProvider(
                DESCRIPTOR, directoryStructureFactory, fileSystemAbstraction, config, tokenHolders,
                directoryFactory, operationalMode, scheduler, auxiliaryTransactionStateManager, log );

        String procedureRegistrationFailureMessage = "Failed to register the fulltext index procedures. The fulltext index provider will be loaded and " +
                "updated like normal, but it might not be possible to query any fulltext indexes. The reason given is: ";
        try
        {
            dependencies.procedures().registerComponent( FulltextAdapter.class, procContext -> provider, true );
            dependencies.procedures().registerProcedure( FulltextProcedures.class );
        }
        catch ( KernelException e )
        {
            String message = procedureRegistrationFailureMessage + e.getUserMessage( new NonTransactionalTokenNameLookup( tokenHolders ) );
            // We use the 'warn' logger in this case, because it can occur due to multi-database shenanigans, or due to internal restarts in HA.
            // These scenarios are less serious, and will _probably_ not prevent FTS from working. Hence we only warn about this.
            logDependencyException( context, log.warnLogger(), message );
        }
        catch ( UnsatisfiedDependencyException e )
        {
            String message = procedureRegistrationFailureMessage + e.getMessage();
            logDependencyException( context, log.errorLogger(), message );
        }

        return provider;
    }

    private void logDependencyException( KernelContext context, Logger dbmsLog, String message )
    {
        // We can for instance get unsatisfied dependency exceptions when the kernel extension is created as part of a consistency check run.
        // In such cases, we will be running in a TOOL context, and we ignore such exceptions since they are harmless.
        // Tools only read, and don't run queries, so there is no need for these advanced pieces of infrastructure.
        if ( context.databaseInfo() != DatabaseInfo.TOOL )
        {
            // If we are not in a "TOOL" context, then we log this at the "DBMS" level, since it might be important for correctness.
            dbmsLog.log( message );
        }
    }
}
