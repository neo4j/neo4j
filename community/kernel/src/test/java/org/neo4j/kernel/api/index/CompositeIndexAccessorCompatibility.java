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
package org.neo4j.kernel.api.index;

import org.junit.Test;

import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptorFactory;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.api.schema_new.IndexQuery.exact;
import static org.neo4j.kernel.api.schema_new.IndexQuery.exists;

public class CompositeIndexAccessorCompatibility extends IndexAccessorCompatibility
{
    protected IndexAccessor accessor;

    public CompositeIndexAccessorCompatibility( IndexProviderCompatibilityTestSuite testSuite )
    {
        super( testSuite, NewIndexDescriptorFactory.forLabel( 1000, 100, 200, 300 ), false );
    }

    @Test
    public void testIndexSeekAndScan() throws Exception
    {
        updateAndCommit( asList( IndexEntryUpdate.add( 1L, descriptor, "a", "a" ),
                IndexEntryUpdate.add( 2L, descriptor, "a", "a" ), IndexEntryUpdate.add( 3L, descriptor, "b", "b" ) ) );

        assertThat( query( exact( 0, "a" ), exact( 1, "a" ) ), equalTo( asList( 1L, 2L ) ) );
        assertThat( query( exists( 1 ) ), equalTo( asList( 1L, 2L, 3L ) ) );
    }

//TODO: add when supported:
    //testIndexSeekByNumber
    //testIndexSeekByString
    //testIndexSeekByPrefix
    //testIndexSeekByPrefixOnNonStrings
}
