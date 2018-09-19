/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.internal.kernel.api.IndexOrder;
import org.neo4j.internal.kernel.api.IndexQuery;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexProgressor;
import org.neo4j.storageengine.api.schema.IndexProgressor.NodeValueClient;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Injects missing property values by lookup in the property store.
 */
class NodeValueInjector implements NodeValueClient, IndexProgressor
{
    private final NodeValueClient target;
    private final NodeCursor node;
    private final PropertyCursor property;
    private final int[] propertyKeyIds;
    private final Read read;
    private IndexProgressor progressor;

    NodeValueInjector( NodeValueClient target,
                       NodeCursor node,
                       PropertyCursor property,
                       Read read,
                       int[] propertyKeyIds )
    {
        this.target = target;
        this.node = node;
        this.property = property;
        this.propertyKeyIds = propertyKeyIds;
        this.read = read;
    }

    @Override
    public void initialize( IndexDescriptor descriptor,
                            IndexProgressor progressor,
                            IndexQuery[] query,
                            IndexOrder indexOrder,
                            boolean needsValues )
    {
        Preconditions.checkArgument( needsValues, "No point in injecting values that are not needed!" );

        this.progressor = progressor;
        target.initialize( descriptor, this, query, indexOrder, needsValues );
    }

    @Override
    public boolean acceptNode( long reference, Value[] values )
    {
        Preconditions.checkArgument( values != null, "Can only inject missing parts, not whole value array!" );

        int nMissing = countMissingValues( values );
        if ( nMissing > 0 )
        {
            // Initialize the property cursor scan
            read.singleNode( reference, node );
            if ( !node.next() )
            {
                return false;
            }

            node.properties( property );
            while ( nMissing > 0 && property.next() )
            {
                for ( int i = 0; i < propertyKeyIds.length; i++ )
                {
                    Value value = values[i];
                    if ( ( value == null || value == NO_VALUE ) && property.propertyKey() == propertyKeyIds[i] )
                    {
                        values[i] = property.propertyValue();
                        nMissing--;
                    }
                }
            }
        }

        return target.acceptNode( reference, values );
    }

    private int countMissingValues( Value[] values )
    {
        int nMissing = 0;
        for ( Value v : values )
        {
            if ( v == null || v == NO_VALUE )
            {
                nMissing++;
            }
        }
        return nMissing;
    }

    @Override
    public boolean needsValues()
    {
        return true;
    }

    @Override
    public boolean next()
    {
        return progressor.next();
    }

    @Override
    public void close()
    {
        node.close();
        property.close();
        progressor.close();
    }
}
