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
package org.neo4j.kernel.impl.transaction.state.storeview;

import org.junit.jupiter.api.Test;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexingService.IndexProxyProvider;
import org.neo4j.kernel.impl.index.schema.LabelScanStore;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes;

class IndexStoreViewFactoryTest
{

    FullScanStoreView fullScanStoreView = mock( FullScanStoreView.class );
    LabelScanStore labelScanStore = mock( LabelScanStore.class );
    LockService lockService = mock( LockService.class );
    LogProvider logProvider = mock( LogProvider.class );
    IndexProxyProvider indexProxies = mock( IndexProxyProvider.class );

    @Test
    void shouldCreateIndexStoreView()
    {
        //Given
        var config = Config.newBuilder().set( enable_scan_stores_as_token_indexes, true ).build();
        var factory = new IndexStoreViewFactory( config, () -> null, fullScanStoreView, labelScanStore, lockService, logProvider );

        //When
        var indexStoreView = factory.createTokenIndexStoreView( indexProxies );

        //Then
        assertThat( indexStoreView.getClass() ).isEqualTo( DynamicIndexStoreView.class );
    }

    @Test
    void shouldCreateLegacyIndexStoreView()
    {
        //Given
        var config = Config.newBuilder().set( enable_scan_stores_as_token_indexes, false ).build();
        var factory = new IndexStoreViewFactory( config, () -> null, fullScanStoreView, labelScanStore, lockService, logProvider );

        //When
        var indexStoreView = factory.createTokenIndexStoreView( indexProxies );

        //Then
        assertThat( indexStoreView.getClass() ).isEqualTo( LegacyDynamicIndexStoreView.class );
    }
}
