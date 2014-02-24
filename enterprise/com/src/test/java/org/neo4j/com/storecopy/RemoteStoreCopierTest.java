/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.com.storecopy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerUtil;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.test.TargetDirectory;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;

public class RemoteStoreCopierTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.cleanTestDirForTest( getClass() );
    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();

    @Test
    public void shouldStoreLogFilesAndRunRecovery() throws Exception
    {
        // Given
        final String copyDir = new File(testDir.directory(), "copy").getAbsolutePath();
        final String originalDir = new File(testDir.directory(), "original").getAbsolutePath();
        Config config = new Config( MapUtil.stringMap( store_dir.name(), copyDir ) );
        RemoteStoreCopier copier = new RemoteStoreCopier( config, loadKernelExtensions(), new ConsoleLogger( StringLogger.SYSTEM ), fs );

        // When
        copier.copyStore( new RemoteStoreCopier.StoreCopyRequester()
        {
            @Override
            public Response<?> copyStore( StoreWriter writer )
            {
                GraphDatabaseAPI original = (GraphDatabaseAPI)new GraphDatabaseFactory().newEmbeddedDatabase( originalDir );
                try
                {
                    // Data that should be available in the store files
                    try( Transaction tx = original.beginTx() )
                    {
                        original.createNode( label( "BeforeCopyBegins" ) );
                        tx.success();
                    }

                    XaDataSourceManager dsManager = original.getDependencyResolver().resolveDependency(
                            XaDataSourceManager.class );
                    RequestContext ctx = ServerUtil.rotateLogsAndStreamStoreFiles( originalDir,
                            dsManager,
                            original.getDependencyResolver().resolveDependency( KernelPanicEventGenerator.class ),
                            StringLogger.SYSTEM, false, writer, fs );

                    // Data that should be made available as part of recovery
                    try( Transaction tx = original.beginTx() )
                    {
                        original.createNode( label( "AfterCopy" ));
                        tx.success();
                    }

                    return ServerUtil.packResponse( original.storeId(), dsManager, ctx, null, ServerUtil.ALL );
                } finally
                {
                    original.shutdown();
                }
            }
        });

        // Then
        GraphDatabaseService copy = new GraphDatabaseFactory().newEmbeddedDatabase( copyDir );

        try( Transaction tx = copy.beginTx() )
        {
            GlobalGraphOperations globalOps = GlobalGraphOperations.at( copy );
            assertThat( Iterables.single(globalOps.getAllNodesWithLabel( label( "BeforeCopyBegins" ) )).getId(), equalTo(0l) );
            assertThat( Iterables.single(globalOps.getAllNodesWithLabel( label( "AfterCopy" ) )).getId(), equalTo(1l) );
            tx.success();
        }

    }

    private List<KernelExtensionFactory<?>> loadKernelExtensions()
    {
        List<KernelExtensionFactory<?>> kernelExtensions = new ArrayList<>();
        for ( KernelExtensionFactory factory : Service.load( KernelExtensionFactory.class ) )
        {
            kernelExtensions.add( factory );
        }
        return kernelExtensions;
    }

}
