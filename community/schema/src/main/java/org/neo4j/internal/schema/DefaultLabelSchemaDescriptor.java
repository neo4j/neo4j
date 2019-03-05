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
package org.neo4j.internal.schema;

import org.apache.commons.lang3.ArrayUtils;

import java.util.Arrays;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.token.api.TokenIdPrettyPrinter;

public class DefaultLabelSchemaDescriptor implements LabelSchemaDescriptor
{
    private final int labelId;
    private final int[] propertyIds;

    DefaultLabelSchemaDescriptor( int labelId, int... propertyIds )
    {
        this.labelId = labelId;
        this.propertyIds = propertyIds;
    }

    @Override
    public boolean isAffected( long[] entityTokenIds )
    {
        return ArrayUtils.contains( entityTokenIds, labelId );
    }

    @Override
    public <R> R computeWith( SchemaComputer<R> processor )
    {
        return processor.computeSpecific( this );
    }

    @Override
    public void processWith( SchemaProcessor processor )
    {
        processor.processSpecific( this );
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return String.format( ":%s(%s)", tokenNameLookup.labelGetName( labelId ),
                TokenIdPrettyPrinter.niceProperties( tokenNameLookup, propertyIds ) );
    }

    @Override
    public int getLabelId()
    {
        return labelId;
    }

    @Override
    public int keyId()
    {
        return getLabelId();
    }

    @Override
    public ResourceType keyType()
    {
        return ResourceTypes.LABEL;
    }

    @Override
    public EntityType entityType()
    {
        return EntityType.NODE;
    }

    @Override
    public PropertySchemaType propertySchemaType()
    {
        return PropertySchemaType.COMPLETE_ALL_TOKENS;
    }

    @Override
    public int[] getPropertyIds()
    {
        return propertyIds;
    }

    @Override
    public int[] getEntityTokenIds()
    {
        return new int[]{labelId};
    }

    @Override
    public boolean isFulltextIndex()
    {
        return false;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o instanceof LabelSchemaDescriptor )
        {
            LabelSchemaDescriptor that = (LabelSchemaDescriptor)o;
            return labelId == that.getLabelId() && Arrays.equals( propertyIds, that.getPropertyIds() );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Arrays.hashCode( propertyIds ) + 31 * labelId;
    }

    @Override
    public String toString()
    {
        return "LabelSchemaDescriptor( " + userDescription( TokenNameLookup.idTokenNameLookup ) + " )";
    }

    @Override
    public LabelSchemaDescriptor schema()
    {
        return this;
    }
}
