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
package org.neo4j.fleetmanagement.queries.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.QueryLanguage;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;

class AggregatedQueriesTimeSliceTest {

    @Test
    void shouldAddQueryEvenIfObfuscatedTextIsEmpty() {
        AggregatedQueriesTimeSlice timeSlice = new AggregatedQueriesTimeSlice();
        ExecutingQuery query = mock(ExecutingQuery.class);
        QuerySnapshot snapshot = mock(QuerySnapshot.class);
        when(query.snapshot()).thenReturn(snapshot);
        when(snapshot.databaseId()).thenReturn(Optional.empty());
        when(snapshot.transactionAnnotationData()).thenReturn(Collections.emptyMap());

        when(query.fullyObfuscatedQueryText()).thenReturn("");
        when(query.queryLanguage()).thenReturn(QueryLanguage.CYPHER_5);

        timeSlice.add(query, null);

        assertThat(timeSlice.size()).isOne();
        UniqueKey key = timeSlice.getAggregations().keySet().iterator().next();
        assertThat(key.getQueryText()).isEmpty();
    }

    @Test
    void shouldAddQueryIfObfuscatedTextIsPresent() {
        AggregatedQueriesTimeSlice timeSlice = new AggregatedQueriesTimeSlice();
        ExecutingQuery query = mock(ExecutingQuery.class);
        QuerySnapshot snapshot = mock(QuerySnapshot.class);
        when(query.snapshot()).thenReturn(snapshot);
        when(snapshot.databaseId()).thenReturn(Optional.empty());
        when(snapshot.transactionAnnotationData()).thenReturn(Collections.emptyMap());

        String obfuscatedText = "MATCH (n) RETURN n";
        when(query.fullyObfuscatedQueryText()).thenReturn(obfuscatedText);
        when(query.queryLanguage()).thenReturn(QueryLanguage.CYPHER_5);

        timeSlice.add(query, null);

        assertThat(timeSlice.size()).isOne();
        UniqueKey key = timeSlice.getAggregations().keySet().iterator().next();
        assertThat(key.getQueryText()).isEqualTo(obfuscatedText);
    }

    @Test
    void shouldAggregateToEmptyTextWhenObfuscationFailed() {
        AggregatedQueriesTimeSlice timeSlice = new AggregatedQueriesTimeSlice();
        ExecutingQuery query = mock(ExecutingQuery.class);
        QuerySnapshot snapshot = mock(QuerySnapshot.class);
        when(query.snapshot()).thenReturn(snapshot);
        when(snapshot.databaseId()).thenReturn(Optional.empty());
        when(snapshot.transactionAnnotationData()).thenReturn(Collections.emptyMap());

        // Obfuscation failed: fullyObfuscatedQueryText() surfaces that as "" (never the raw query text), so the
        // aggregation key must collapse to "".
        when(query.fullyObfuscatedQueryText()).thenReturn("");
        when(query.queryLanguage()).thenReturn(QueryLanguage.CYPHER_5);

        timeSlice.add(query, null);

        assertThat(timeSlice.size()).isOne();
        UniqueKey key = timeSlice.getAggregations().keySet().iterator().next();
        assertThat(key.getQueryText()).isEmpty();
    }
}
