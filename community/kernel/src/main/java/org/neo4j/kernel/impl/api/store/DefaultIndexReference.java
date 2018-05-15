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
package org.neo4j.kernel.impl.api.store;

import java.util.Arrays;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;

public class DefaultIndexReference implements IndexReference
{
    private final boolean unique;
    private final int label;
    private final int[] properties;

    private DefaultIndexReference( boolean unique, int label, int[] properties )
    {
        this.unique = unique;
        this.label = label;
        this.properties = properties;
    }

    @Override
    public boolean isUnique()
    {
        return unique;
    }

    @Override
    public int label()
    {
        return label;
    }

    @Override
    public int[] properties()
    {
        return properties;
    }

    @Override
    public String providerKey()
    {
        throw new UnsupportedOperationException( "not yet" );
    }

    @Override
    public String providerVersion()
    {
        throw new UnsupportedOperationException( "not yet" );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || !( o instanceof IndexReference ) )
        {
            return false;
        }

        IndexReference that = (IndexReference) o;
        return unique == that.isUnique() && label == that.label() && Arrays.equals( properties, that.properties() );
    }

    @Override
    public int hashCode()
    {
        int result = unique ? 1 : 0;
        result = 31 * result + label;
        result = 31 * result + Arrays.hashCode( properties );
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "Index(%d:%s)", label, Arrays.toString( properties ) );
    }

}
