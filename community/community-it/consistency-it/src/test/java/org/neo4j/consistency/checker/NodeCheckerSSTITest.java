/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.consistency.checker;

import java.io.IOException;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.IndexProxy;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.kernel.impl.index.schema.TokenScanWriter;
import org.neo4j.storageengine.api.EntityTokenUpdate;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class NodeCheckerSSTITest extends NodeCheckerTest
{
    @Override
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
    }

    @Override
    Config additionalConfigToCC( Config config )
    {
        config.set( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
        return config;
    }

    @Override
    TokenScanWriter labelIndexWriter()
    {
        IndexingService indexingService = db.getDependencyResolver().resolveDependency( IndexingService.class );
        final IndexDescriptor[] indexDescriptors =
                schemaStorage.indexGetForSchema( SchemaDescriptor.forAllEntityTokens( EntityType.NODE ), PageCursorTracer.NULL );
        // The Node Label Index should exist and be unique.
        assertThat( indexDescriptors.length ).isEqualTo( 1 );
        IndexDescriptor nli = indexDescriptors[0];
        IndexProxy indexProxy;
        try
        {
            indexProxy = indexingService.getIndexProxy( nli );
        } catch ( IndexNotFoundKernelException e )
        {
            throw new RuntimeException( e );
        }
        IndexUpdater indexUpdater = indexProxy.newUpdater( IndexUpdateMode.ONLINE, PageCursorTracer.NULL );
        return new TokenScanWriter()
        {
            @Override
            public void write( EntityTokenUpdate update )
            {
                try
                {
                    indexUpdater.process( IndexEntryUpdate.change( update.getEntityId(), nli, update.getTokensBefore(), update.getTokensAfter() ) );
                }
                catch ( IndexEntryConflictException e )
                {
                    //The TokenIndexUpdater should never throw IndexEntryConflictException.
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void close()
            {
                try
                {
                    indexUpdater.close();
                }
                catch ( IndexEntryConflictException e )
                {
                    //The TokenIndexUpdater should never throw IndexEntryConflictException.
                    throw new RuntimeException( e );
                }
            }
        };
    }
}
