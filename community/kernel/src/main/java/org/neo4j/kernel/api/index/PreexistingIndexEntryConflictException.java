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

import static java.lang.String.format;

/**
 * Thrown from update methods (eg. {@link IndexPopulator#add(long, Object)}, {@link IndexPopulator#update(Iterable)},
 * and {@link IndexAccessor#updateAndCommit(Iterable)}) of an index that is unique when a conflicting entry (clashing
 * with an existing value - violating uniqueness) is being added.
 */
public class PreexistingIndexEntryConflictException extends IndexEntryConflictException
{
    private final Object propertyValue;
    private final long addedNodeId;
    private final long existingNodeId;

    public PreexistingIndexEntryConflictException( Object propertyValue, long existingNodeId, long addedNodeId )
    {
        super( format( "Multiple nodes have property value %s:%n" +
                "  node(%d)%n" +
                "  node(%d)",
                quote( propertyValue ), existingNodeId, addedNodeId ) );
        this.addedNodeId = addedNodeId;
        this.propertyValue = propertyValue;
        this.existingNodeId = existingNodeId;
    }

    @Override
    public Object getPropertyValue()
    {
        return propertyValue;
    }

    @Override
    public String evidenceMessage( String labelName, String propertyKey )
    {
        return format(
                "Multiple nodes with label `%s` have property `%s` = %s:%n" +
                        "  node(%d)%n" +
                        "  node(%d)",
                labelName, propertyKey, quote( propertyValue ), existingNodeId, addedNodeId );
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

        PreexistingIndexEntryConflictException that = (PreexistingIndexEntryConflictException) o;

        return addedNodeId == that.addedNodeId &&
                existingNodeId == that.existingNodeId &&
                !(propertyValue != null ? !propertyValue.equals( that.propertyValue ) : that.propertyValue != null);
    }

    @Override
    public int hashCode()
    {
        int result = propertyValue != null ? propertyValue.hashCode() : 0;
        result = 31 * result + (int) (addedNodeId ^ (addedNodeId >>> 32));
        result = 31 * result + (int) (existingNodeId ^ (existingNodeId >>> 32));
        return result;
    }

    @Override
    public String toString()
    {
        return "PreexistingIndexEntryConflictException{" +
                "propertyValue=" + propertyValue +
                ", addedNodeId=" + addedNodeId +
                ", existingNodeId=" + existingNodeId +
                '}';
    }
}
