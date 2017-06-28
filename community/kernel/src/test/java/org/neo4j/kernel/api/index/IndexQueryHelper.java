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

import org.neo4j.kernel.api.schema.IndexQuery;
import org.neo4j.kernel.api.schema.LabelSchemaDescriptor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class IndexQueryHelper
{
    private IndexQueryHelper()
    {
    }

    public static IndexQuery exact( int propertyKeyId, Object value )
    {
        return IndexQuery.exact( propertyKeyId, Values.of( value ) );
    }

    public static IndexEntryUpdate<LabelSchemaDescriptor> add(
            long nodeId, LabelSchemaDescriptor schema, Object... objects )
    {
        return IndexEntryUpdate.add( nodeId, schema, toValues( objects ) );
    }

    public static IndexEntryUpdate<LabelSchemaDescriptor> remove(
            long nodeId, LabelSchemaDescriptor schema, Object... objects )
    {
        return IndexEntryUpdate.remove( nodeId, schema, toValues( objects ) );
    }

    public static IndexEntryUpdate<LabelSchemaDescriptor> change(
            long nodeId, LabelSchemaDescriptor schema, Object o1, Object o2 )
    {
        return IndexEntryUpdate.change( nodeId, schema, Values.of( o1 ), Values.of( o2 ) );
    }

    public static IndexEntryUpdate<LabelSchemaDescriptor> change(
            long nodeId, LabelSchemaDescriptor schema, Object[] o1, Object[] o2 )
    {
        return IndexEntryUpdate.change( nodeId, schema, toValues( o1 ), toValues( o2 ) );
    }

    private static Value[] toValues( Object[] objects )
    {
        Value[] values = new Value[objects.length];
        for ( int i = 0; i < objects.length; i++ )
        {
            values[i] = Values.of( objects[i] );
        }
        return values;
    }
}
