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
package org.neo4j.kernel.impl.store.record;

import java.nio.ByteBuffer;

import org.neo4j.kernel.api.schema.NodeMultiPropertyDescriptor;
import org.neo4j.kernel.api.schema.NodePropertyDescriptor;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;

import static org.neo4j.kernel.impl.util.IoPrimitiveUtils.safeCastLongToInt;

public class UniquePropertyConstraintRule extends NodePropertyConstraintRule
{
    private final long ownedIndexRule;

    public static UniquePropertyConstraintRule uniquenessConstraintRule( long id, NodePropertyDescriptor descriptor,
                                                                         long ownedIndexRule )
    {
        return new UniquePropertyConstraintRule( id, descriptor, ownedIndexRule );
    }

    public static UniquePropertyConstraintRule readUniquenessConstraintRule( long id, int labelId, ByteBuffer buffer )
    {
        return new UniquePropertyConstraintRule( id, readDescriptor( labelId, buffer ), readOwnedIndexRule( buffer ) );
    }

    private UniquePropertyConstraintRule( long id, NodePropertyDescriptor descriptor, long ownedIndexRule )
    {
        super( id, descriptor, Kind.UNIQUENESS_CONSTRAINT );
        this.ownedIndexRule = ownedIndexRule;
        assert !descriptor.isComposite(); // Only uniqueness of a single property supported for now
    }

    @Override
    public String toString()
    {
        return "UniquePropertyConstraintRule[id=" + id + ", label=" + descriptor().getLabelId() + ", kind=" + kind +
               ", propertyKeys=" + descriptor.propertyIdText() + ", ownedIndex=" + ownedIndexRule + "]";
    }

    @Override
    public int length()
    {
        //TODO: Change format to rather use short/int for length/propertyId, much like count store does
        int propertyCount = descriptor().isComposite() ? descriptor.getPropertyKeyIds().length : 1;
        return 4 /* label */ +
               1 /* kind id */ +
               1 +  /* the number of properties that form a unique tuple */
               8 * propertyCount + /* the property keys themselves */
               8; /* owned index rule */
    }

    @Override
    public void serialize( ByteBuffer target )
    {
        int[] propertyKeyIds =
                descriptor().isComposite() ? descriptor.getPropertyKeyIds() : new int[]{descriptor.getPropertyKeyId()};
        target.putInt( descriptor().getLabelId() );
        target.put( kind.id() );
        target.put( (byte) propertyKeyIds.length );
        for ( int propertyKeyId : propertyKeyIds )
        {
            target.putLong( propertyKeyId );
        }
        target.putLong( ownedIndexRule );
    }

    private static NodePropertyDescriptor readDescriptor(int labelId, ByteBuffer buffer)
    {
        int[] keys = new int[buffer.get()];
        if ( keys.length > 1 )
        {
            for ( int i = 0; i < keys.length; i++ )
            {
                keys[i] = safeCastLongToInt( buffer.getLong() );
            }
            return new NodeMultiPropertyDescriptor( labelId, keys );
        }
        else
        {
            return new NodePropertyDescriptor( labelId, safeCastLongToInt( buffer.getLong() ) );
        }
    }

    private static long readOwnedIndexRule( ByteBuffer buffer )
    {
        return buffer.getLong();
    }

    public long getOwnedIndex()
    {
        return ownedIndexRule;
    }

    @Override
    public UniquenessConstraint toConstraint()
    {
        return new UniquenessConstraint( descriptor() );
    }

}
