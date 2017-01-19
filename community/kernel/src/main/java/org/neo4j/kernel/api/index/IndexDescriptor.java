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

import org.neo4j.kernel.api.schema.NodePropertyDescriptor;

import static java.lang.String.format;

/**
 * This is a new version of the old IndexDescriptor interface created here only to allow
 * older versions of Cypher to run on newer kernels. This is because Cypher supports older
 * cypher-compiler modules to be instantiated and run within newer versions of Neo4j, and they
 * assume certain kernel API's remain unchanged. However, the IndexDescriptor was refactored
 * considerably during the 3.2 development process to allow for composite indexes and constraints.
 * We should remove this class as soon as older Cypher compilers are re-released without any
 * kernel dependency on this.
 * <p>
 * //TODO: Delete this class when 3.1.1 and 2.3.9 are released
 */
@Deprecated
public class IndexDescriptor
{
    private NodePropertyDescriptor descriptor;

    public IndexDescriptor( int labelId, int propertyKeyId )
    {
        this.descriptor = new NodePropertyDescriptor( labelId, propertyKeyId );
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( obj != null && obj instanceof IndexDescriptor )
        {
            IndexDescriptor that = (IndexDescriptor) obj;
            return this.descriptor.equals( that.descriptor );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.descriptor.hashCode();
    }

    /**
     * @return label token id this index is for.
     */
    public int getLabelId()
    {
        return descriptor.getLabelId();
    }

    /**
     * @return property key token id this index is for.
     */
    public int getPropertyKeyId()
    {
        return descriptor.getPropertyKeyId();
    }

    @Override
    public String toString()
    {
        return format( ":label[%d](property[%d])", descriptor.getLabelId(), descriptor.getPropertyKeyId() );
    }
}
