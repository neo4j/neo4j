/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import static java.lang.String.format;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

public class DuplicateIndexEntryConflictExceptionTest
{
    @Test
    public void messageShouldIncludePropertyValueAndNodeIds() throws Exception
    {
        // given
        DuplicateIndexEntryConflictException e = new DuplicateIndexEntryConflictException( "value1", asSet(11l, 22l, 33l) );

        // then
        assertEquals( format( "Multiple nodes have property value 'value1':%n" +
                "  node(11), node(22), node(33)" ), e.getMessage());
    }

    @Test
    public void evidenceMessageShouldIncludeLabelAndPropertyKey() throws Exception
    {
        // given
        DuplicateIndexEntryConflictException e = new DuplicateIndexEntryConflictException( "value1", asSet(11l, 22l, 33l) );

        // then
        assertEquals( format( "Multiple nodes with label `Label1` have property `propertyKey1` = 'value1':%n" +
                "  node(11), node(22), node(33)" ), e.evidenceMessage( "Label1", "propertyKey1" ));
    }
}
