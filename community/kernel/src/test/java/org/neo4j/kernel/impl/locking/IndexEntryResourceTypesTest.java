/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.locking;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.IndexQuery.ExactPredicate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.Iterators.array;

public class IndexEntryResourceTypesTest
{

    public static final int labelId = 1;
    public static final int propertyId = 2;
    public static final String value = "value";

    @Test
    public void shouldProduceBackwardsCompatibleId()
    {
        long id = ResourceTypes.indexEntryResourceId( labelId, IndexQuery.exact( propertyId, value ) );
        assertThat( id, equalTo( 155667838465249649L ) );
    }

    @Test
    public void shouldDifferentiateBetweenIndexes()
    {

        ExactPredicate pred1 = IndexQuery.exact( 1, "value" );
        ExactPredicate pred2 = IndexQuery.exact( 1, "value2" );
        ExactPredicate pred3 = IndexQuery.exact( 2, "value" );
        ExactPredicate pred4 = IndexQuery.exact( 2, "value2" );

        List<Long> ids = Arrays.asList(
                        ResourceTypes.indexEntryResourceId( 1, array( pred1 ) ),
                        ResourceTypes.indexEntryResourceId( 1, array( pred2 ) ),
                        ResourceTypes.indexEntryResourceId( 1, array( pred3 ) ),
                        ResourceTypes.indexEntryResourceId( 1, array( pred4 ) ),
                        ResourceTypes.indexEntryResourceId( 2, array( pred1 ) ),
                        ResourceTypes.indexEntryResourceId( 1, array( pred1, pred2 ) ),
                        ResourceTypes.indexEntryResourceId( 1, array( pred1, pred2, pred3 ) ),
                        ResourceTypes.indexEntryResourceId( 2, array( pred1, pred2, pred3, pred4 ) ) );

        Set<Long> uniqueIds = Iterables.asSet( ids );
        assertThat( ids.size(), equalTo( uniqueIds.size() ) );
    }
}
