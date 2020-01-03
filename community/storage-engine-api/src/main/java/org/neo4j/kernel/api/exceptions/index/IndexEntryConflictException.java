/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import java.util.Arrays;
import java.util.stream.Collectors;

import org.neo4j.common.TokenNameLookup;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueTuple;

import static java.lang.String.format;
import static org.neo4j.kernel.api.StatementConstants.NO_SUCH_NODE;

public class IndexEntryConflictException extends KernelException
{
    private final ValueTuple propertyValues;
    private final long addedNodeId;
    private final long existingNodeId;

    /**
     * Make IOUtils happy
     */
    public IndexEntryConflictException( String message, Throwable cause )
    {
        super( Status.Schema.ConstraintViolation, message, cause );
        propertyValues = null;
        addedNodeId = -1;
        existingNodeId = -1;
    }

    public IndexEntryConflictException( long existingNodeId, long addedNodeId, Value... propertyValue )
    {
        this( existingNodeId, addedNodeId, ValueTuple.of( propertyValue ) );
    }

    public IndexEntryConflictException( long existingNodeId, long addedNodeId, ValueTuple propertyValues )
    {
        super( Status.Schema.ConstraintViolation, "Both node %d and node %d share the property value %s", existingNodeId, addedNodeId, propertyValues );
        this.existingNodeId = existingNodeId;
        this.addedNodeId = addedNodeId;
        this.propertyValues = propertyValues;
    }

    public String evidenceMessage( TokenNameLookup tokenNameLookup, SchemaDescriptor schema )
    {
        assert schema.getPropertyIds().length == propertyValues.size();

        String labelName = Arrays.stream( schema.getEntityTokenIds() )
                .mapToObj( tokenNameLookup::labelGetName )
                .collect( Collectors.joining( "`, `", "`", "`") );
        if ( addedNodeId == NO_SUCH_NODE )
        {
            return format( "Node(%d) already exists with label %s and %s",
                    existingNodeId, labelName, propertyString( tokenNameLookup, schema.getPropertyIds() ) );
        }
        else
        {
            return format( "Both Node(%d) and Node(%d) have the label %s and %s",
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
