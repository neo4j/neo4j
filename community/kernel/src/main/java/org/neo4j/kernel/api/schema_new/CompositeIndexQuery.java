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
package org.neo4j.kernel.api.schema_new;

import org.neo4j.kernel.api.exceptions.index.IndexNotApplicableKernelException;
import org.neo4j.kernel.api.schema_new.index.NewIndexDescriptor;

/**
 * This object is used internally to represent index queries on multiple values.
 * <p>
 * See {@link org.neo4j.kernel.api.ReadOperations#nodesGetFromIndexSeek(NewIndexDescriptor, Object)}
 */
public class CompositeIndexQuery
{
    private final Object[] values;
    private final int[] propertyIds;

    public CompositeIndexQuery( Object[] values, int[] propertyIds )
    {
        this.values = values;
        this.propertyIds = propertyIds;
    }

    public Object[] validate( NewIndexDescriptor index ) throws IndexNotApplicableKernelException
    {
        Object[] orderdValues = new Object[propertyIds.length];
        int[] propertyIdsFromIndex = index.schema().getPropertyIds();
        if ( propertyIdsFromIndex.length != propertyIds.length )
        {
            throw new IndexNotApplicableKernelException(
                    String.format( "Expected %d properties, but found %d", propertyIdsFromIndex.length,
                            propertyIds.length ) );
        }
        for ( int i = 0; i < propertyIdsFromIndex.length; i++ )
        {
            for ( int j = 0; j < propertyIds.length; j++ )
            {
                if ( propertyIdsFromIndex[i] == propertyIds[j] )
                {
                    orderdValues[i] = values[j];
                }
            }
            if ( orderdValues[i] == null )
            {
                throw new IndexNotApplicableKernelException(
                        String.format( "PropertyId %d from index %s not supplied.", propertyIdsFromIndex[i],
                                index.toString() ) );
            }
        }
        return orderdValues;
    }
}
