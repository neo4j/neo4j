/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.boltmessages.request.connection.RoutingContext;
import org.neo4j.fabric.eval.Catalog;
import org.neo4j.fabric.eval.CatalogManager;
import org.neo4j.fabric.executor.Location;
import org.neo4j.kernel.database.DatabaseReference;

class LocationCacheTest {
    private Catalog.Graph graph0 = mock(Catalog.Graph.class);
    private Location location1 = mock(Location.class);
    private Catalog.Graph graph1 = mock(Catalog.Graph.class);
    private Location location2 = mock(Location.class);
    private Catalog.Graph graph2 = mock(Catalog.Graph.class);

    private CatalogManager catalogManager;

    private LocationCache locationCache;

    @BeforeEach
    void setUp() {

        // given
        catalogManager = mock(CatalogManager.class);
        var transactionInfo = mock(FabricTransactionInfo.class);
        var routingContext = mock(RoutingContext.class);
        when(transactionInfo.getRoutingContext()).thenReturn(routingContext);
        locationCache = new LocationCache(catalogManager, transactionInfo);

        // when
        when(catalogManager.locationOf(nullable(DatabaseReference.class), eq(graph1), any(Boolean.class), any()))
                .thenReturn(location1);
        when(catalogManager.locationOf(nullable(DatabaseReference.class), eq(graph2), eq(true), any()))
                .thenReturn(location2);
    }

    @Test
    void shouldHandleNonExistentGraph() {
        assertThat(locationCache.locationOf(graph0, false)).isNull();
        verify(catalogManager, times(1))
                .locationOf(nullable(DatabaseReference.class), any(Catalog.Graph.class), any(Boolean.class), any());
        assertThat(locationCache.size()).isZero();
    }

    @Test
    void shouldCacheGraph() {
        assertThat(locationCache.locationOf(graph1, false)).isEqualTo(location1);
        assertThat(locationCache.locationOf(graph1, false)).isEqualTo(location1);
        verify(catalogManager, times(1))
                .locationOf(nullable(DatabaseReference.class), any(Catalog.Graph.class), any(Boolean.class), any());
        assertThat(locationCache.size()).isOne();
    }

    @Test
    void shouldNotDistinguishReadWrite() {
        assertThat(locationCache.locationOf(graph1, false)).isEqualTo(location1);
        assertThat(locationCache.locationOf(graph1, true)).isEqualTo(location1);
        assertThat(locationCache.locationOf(graph1, false)).isEqualTo(location1);
        verify(catalogManager, times(1))
                .locationOf(nullable(DatabaseReference.class), any(Catalog.Graph.class), any(Boolean.class), any());
        assertThat(locationCache.size()).isOne();
    }

    @Test
    void shouldHandleMultipleGraphs() {
        assertThat(locationCache.locationOf(graph1, false)).isEqualTo(location1);
        assertThat(locationCache.locationOf(graph1, true)).isEqualTo(location1);
        assertThat(locationCache.locationOf(graph2, true)).isEqualTo(location2);
        assertThat(locationCache.locationOf(graph2, true)).isEqualTo(location2);
        verify(catalogManager, times(2))
                .locationOf(nullable(DatabaseReference.class), any(Catalog.Graph.class), any(Boolean.class), any());
        assertThat(locationCache.size()).isEqualTo(2);
    }
}
