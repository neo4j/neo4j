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
package org.neo4j.kernel.api.impl;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.Query;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class LuceneTestUtil
{
    public static List<Value[]> valueTupleList( Object... objects )
    {
        return Arrays.stream( objects )
                .map( LuceneTestUtil::valueTuple )
                .collect( Collectors.toList() );
    }

    public static Value[] valueTuple( Object object )
    {
        return new Value[]{Values.of( object )};
    }

    public static Document documentRepresentingProperties( long nodeId, Object... objects )
    {
        return LuceneDocumentStructure.documentRepresentingProperties(
                nodeId,
                Values.values( objects )
        );
    }

    public static Query newSeekQuery( Object... objects )
    {
        return LuceneDocumentStructure.newSeekQuery( Values.values( objects ) );
    }
}
