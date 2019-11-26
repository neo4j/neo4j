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
package org.neo4j.consistency.newchecker;

import org.junit.jupiter.api.Test;

import java.util.function.BiConsumer;

import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.api.TokenHolder;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

class RecordLoadingTest
{
    @Test
    void shouldReturnsFalseOnMissingToken()
    {
        // given
        NodeRecord entity = new NodeRecord( 0 );
        TokenHolder tokenHolder = new DelegatingTokenHolder( new ReadOnlyTokenCreator(), "Test" );
        TokenStore<PropertyKeyTokenRecord> store = mock( TokenStore.class );
        BiConsumer noopReporter = mock( BiConsumer.class );

        // when
        boolean valid = RecordLoading.checkValidToken( entity, 0, tokenHolder, store, noopReporter, noopReporter );

        // then
        assertFalse( valid );
    }
}
