/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.api.index;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.neo4j.helpers.collection.IteratorUtil.asIterable;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import org.junit.Test;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class IndexRuleRepositoryTest
{
    @Test
    public void shouldIncludeIndexRuleAfterItsBeenAdded() throws Exception
    {
        // Given
        IndexRuleRepository repo = new IndexRuleRepository();

        // When
        repo.add( new IndexRule( 1, 10, 100l ) );

        // Then
        assertThat( asSet( asIterable( repo.getIndexedProperties( 10 ) )), equalTo(asSet( 100l )));
    }
}
