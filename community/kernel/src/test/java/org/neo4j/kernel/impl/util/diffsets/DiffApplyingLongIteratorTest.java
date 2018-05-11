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

package org.neo4j.kernel.impl.util.diffsets;

import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.Resource;

import static java.util.Collections.emptySet;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.emptyIterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.resourceIterator;

public class DiffApplyingLongIteratorTest
{
    @Test
    public void closeResource()
    {
        Resource resource = Mockito.mock( Resource.class );
        PrimitiveLongResourceIterator source = resourceIterator( emptyIterator(), resource );

        PrimitiveLongResourceIterator iterator = DiffApplyingLongIterator.augment( source, emptySet(), emptySet() );

        iterator.close();

        Mockito.verify( resource ).close();
    }
}
