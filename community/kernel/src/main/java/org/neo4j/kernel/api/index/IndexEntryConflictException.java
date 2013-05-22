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

import static java.lang.String.format;
import static java.lang.String.valueOf;

public abstract class IndexEntryConflictException extends Exception
{
    public IndexEntryConflictException( String message )
    {
        super( message );
    }

    protected static String quote( Object propertyValue )
    {
        if (propertyValue instanceof String)
        {
            return format( "'%s'", propertyValue );
        }
        return valueOf( propertyValue );
    }

    /**
     * Use this method in cases where {@link org.neo4j.kernel.api.index.IndexEntryConflictException} was caught but it should not have been
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

    public abstract Object getPropertyValue();

    public abstract String evidenceMessage( String labelName, String propertyKey );
}
