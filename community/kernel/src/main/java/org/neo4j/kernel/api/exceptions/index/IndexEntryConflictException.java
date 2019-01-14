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
package org.neo4j.kernel.api.exceptions.index;

import org.neo4j.internal.kernel.api.TokenNameLookup;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

import static java.lang.String.format;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;

/**
 * TODO why isn't this a {@link KernelException}?
 */
public class IndexEntryConflictException extends Exception
{
    private final ValueTuple propertyValues;
    private final long addedNodeId;
    private final long existingNodeId;

    public IndexEntryConflictException( long existingNodeId, long addedNodeId, Value... propertyValue )
    {
        this( existingNodeId, addedNodeId, ValueTuple.of( propertyValue ) );
    }

    public IndexEntryConflictException( long existingNodeId, long addedNodeId, ValueTuple propertyValues )
    {
        super( format( "Both node %d and node %d share the property value %s",
                existingNodeId, addedNodeId, propertyValues ) );
        this.existingNodeId = existingNodeId;
        this.addedNodeId = addedNodeId;
        this.propertyValues = propertyValues;
    }

    /**
     * Use this method in cases where {@link org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException}
     * was caught but it should not have been allowed to be thrown in the first place.
     * Typically where the index we performed an operation on is not a unique index.
     */
    public RuntimeException notAllowed( SchemaIndexDescriptor descriptor )
    {
        return new IllegalStateException( String.format(
                "Index for (%s) should not require unique values.",
                descriptor.userDescription( SchemaUtil.idTokenNameLookup ) ), this );
    }

    public String evidenceMessage( TokenNameLookup tokenNameLookup, SchemaDescriptor schema )
    {
        assert schema.getPropertyIds().length == propertyValues.size();

        String labelName = tokenNameLookup.labelGetName( schema.keyId() );
        if ( addedNodeId == NO_SUCH_NODE )
        {
            return format( "Node(%d) already exists with label `%s` and %s",
                    existingNodeId, labelName, propertyString( tokenNameLookup, schema.getPropertyIds() ) );
        }
        else
        {
            return format( "Both Node(%d) and Node(%d) have the label `%s` and %s",
                    existingNodeId, addedNodeId, labelName, propertyString( tokenNameLookup, schema.getPropertyIds() ) );
        }
    }

    public ValueTuple getPropertyValues()
    {
        return propertyValues;
    }

    public Value getSinglePropertyValue()
    {
        return propertyValues.getOnlyValue();
    }

    public long getAddedNodeId()
    {
        return addedNodeId;
    }

    public long getExistingNodeId()
    {
        return existingNodeId;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        IndexEntryConflictException that = (IndexEntryConflictException) o;

        return addedNodeId == that.addedNodeId &&
                existingNodeId == that.existingNodeId &&
                !(propertyValues != null ? !propertyValues.equals( that.propertyValues ) : that.propertyValues != null);
    }

    @Override
    public int hashCode()
    {
        int result = propertyValues != null ? propertyValues.hashCode() : 0;
        result = 31 * result + (int) (addedNodeId ^ (addedNodeId >>> 32));
        result = 31 * result + (int) (existingNodeId ^ (existingNodeId >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "IndexEntryConflictException{" +
                "propertyValues=" + propertyValues +
                ", addedNodeId=" + addedNodeId +
                ", existingNodeId=" + existingNodeId +
                '}';
    }

    private String propertyString( TokenNameLookup tokenNameLookup, int[] propertyIds )
    {
        StringBuilder sb = new StringBuilder();
        String sep = propertyIds.length > 1 ? "properties " : "property ";
        for ( int i = 0; i < propertyIds.length; i++ )
        {
            sb.append( sep );
            sep = ", ";
            sb.append( '`' );
            sb.append( tokenNameLookup.propertyKeyGetName( propertyIds[i] ) );
            sb.append( "` = " );
            sb.append( propertyValues.valueAt( i ).prettyPrint() );
        }
        return sb.toString();
    }
}
