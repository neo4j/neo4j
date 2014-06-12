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

import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.test.TargetDirectory;

public class RemoteStoreCopierTest
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    private final DefaultFileSystemAbstraction fs = new DefaultFileSystemAbstraction();
/*
    @Test
    public void shouldStoreLogFilesAndRunRecovery() throws Exception
    {
        // Given
        final String copyDir = new File( testDir.directory(), "copy" ).getAbsolutePath();
        final String originalDir = new File( testDir.directory(), "original" ).getAbsolutePath();
        Config config = new Config( MapUtil.stringMap( store_dir.name(), copyDir ) );
        RemoteStoreCopier copier = new RemoteStoreCopier( config, loadKernelExtensions(), new ConsoleLogger( StringLogger.SYSTEM ), fs );

        final GraphDatabaseAPI original = (GraphDatabaseAPI)new GraphDatabaseFactory().newEmbeddedDatabase( originalDir );

        // When
        RemoteStoreCopier.StoreCopyRequester requester = spy( new RemoteStoreCopier.StoreCopyRequester()
        {
            public Response<Object> response;

            @Override
            public Response<?> copyStore( StoreWriter writer )
            {
                // Data that should be available in the store files
                try ( Transaction tx = original.beginTx() )
                {
                    original.createNode( label( "BeforeCopyBegins" ) );
                    tx.success();
                }

                XaDataSourceManager dsManager = original.getDependencyResolver().resolveDependency(
                        XaDataSourceManager.class );
                RequestContext ctx = ServerUtil.rotateLogsAndStreamStoreFiles( originalDir,
                        dsManager,
                        original.getDependencyResolver().resolveDependency( KernelPanicEventGenerator.class ),
                        StringLogger.SYSTEM, false, writer, fs,
                        original.getDependencyResolver().resolveDependency( Monitors.class ).newMonitor(
                                BackupMonitor.class )
                );

                // Data that should be made available as part of recovery
                try ( Transaction tx = original.beginTx() )
                {
                    original.createNode( label( "AfterCopy" ) );
                    tx.success();
                }

                response = spy( ServerUtil.packResponse( original.storeId(), dsManager, ctx, null,
                        ServerUtil.ALL ) );
                return response;
            }

            @Override
            public void done()
            {
                // Ensure response is closed before this method is called
                assertNotNull( response );
                verify( response, times( 1 ) ).close();
            }
        } );
        copier.copyStore( requester );

        // Then
        GraphDatabaseService copy = new GraphDatabaseFactory().newEmbeddedDatabase( copyDir );

        try( Transaction tx = copy.beginTx() )
        {
            GlobalGraphOperations globalOps = GlobalGraphOperations.at( copy );
            assertThat( Iterables.single( globalOps.getAllNodesWithLabel( label( "BeforeCopyBegins" ) ) ).getId(),
                    equalTo( 0l ) );
            assertThat( Iterables.single(globalOps.getAllNodesWithLabel( label( "AfterCopy" ) )).getId(), equalTo(1l) );
            tx.success();
        }
        finally
        {
            copy.shutdown();
            original.shutdown();
        }

        verify( requester, times( 1 ) ).done();
    }
*/
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
