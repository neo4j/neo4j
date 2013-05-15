/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.kernel.impl.api.index.IndexDescriptor;

/**
 * Thrown from update methods (eg. {@link IndexPopulator#add(long, Object)}, {@link IndexPopulator#update(Iterable)},
 * and {@link IndexAccessor#updateAndCommit(Iterable)}) of an index that is unique when a conflicting entry (clashing
 * with an existing value - violating uniqueness) is being added.
 */
public class IndexEntryConflictException extends Exception
{
    private final long addedNodeId;
    private final Object propertyValue;
    private final long existingNodeId;

    public IndexEntryConflictException( long addedNodeId, Object propertyValue, long existingNodeId )
    {
        super( String.format( "Could not index node with id:%d for propertyValue:[%s], " +
                              "the unique index already contains an entry with that value for node with id:%d",
                              addedNodeId, propertyValue, existingNodeId ) );
        this.addedNodeId = addedNodeId;
        this.propertyValue = propertyValue;
        this.existingNodeId = existingNodeId;
    }

    /**
     * Use this method in cases where {@link IndexEntryConflictException} was caught but it should not have been
     * allowed to be thrown in the first place. Typically where the index we performed an operation on is not a
     * unique index.
     */
    public RuntimeException notAllowed( long labelId, long propertyKeyId )
    {
        return new IllegalStateException( String.format(
                "Index for label:%s propertyKey:%s should not require unique values.",
                labelId, propertyKeyId ), this );
    }

    public RuntimeException notAllowed( IndexDescriptor descriptor )
    {
        return notAllowed( descriptor.getLabelId(), descriptor.getPropertyKeyId() );
    }

    public long getAddedNodeId()
    {
        return addedNodeId;
    }

    public long getExistingNodeId()
    {
        return existingNodeId;
    }

    public Object getPropertyValue()
    {
        return propertyValue;
    }
}
