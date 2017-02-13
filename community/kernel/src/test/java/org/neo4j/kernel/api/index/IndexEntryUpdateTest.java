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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class IndexEntryUpdateTest
{
    @Test
    public void indexEntryUpdatesShouldBeEqual()
    {
        IndexEntryUpdate a = IndexEntryUpdate.add( 0, NewIndexDescriptorFactory.forLabel( 3, 4 ), "hi" );
        IndexEntryUpdate b = IndexEntryUpdate.add( 0, NewIndexDescriptorFactory.forLabel( 3, 4 ), "hi" );
        assertThat( a, equalTo( b ) );
        assertThat( a.hashCode(), equalTo( b.hashCode() ) );
    }
}
