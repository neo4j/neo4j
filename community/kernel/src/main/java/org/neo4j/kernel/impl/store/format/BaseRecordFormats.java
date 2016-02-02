/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.store.format;

import org.neo4j.kernel.impl.store.StoreType;

import static org.neo4j.helpers.ArrayUtil.contains;
import static org.neo4j.helpers.ArrayUtil.containsAll;
import static org.neo4j.helpers.ArrayUtil.filter;

/**
 * Base class for simpler implementation of {@link RecordFormats}.
 */
public abstract class BaseRecordFormats implements RecordFormats
{
    @Override
    public boolean equals( Object obj )
    {
        if ( obj == null || !(obj instanceof RecordFormats) )
        {
            return false;
        }

        RecordFormats other = (RecordFormats) obj;
        return  node().equals( other.node() ) &&
                relationship().equals( other.relationship() ) &&
                relationshipGroup().equals( other.relationshipGroup() ) &&
                property().equals( other.property() ) &&
                labelToken().equals( other.labelToken() ) &&
                relationshipTypeToken().equals( other.relationshipTypeToken() ) &&
                propertyKeyToken().equals( other.propertyKeyToken() ) &&
                dynamic().equals( other.dynamic() );
    }

    @Override
    public int hashCode()
    {
        int hashCode = 17;
        hashCode = 31 * hashCode + node().hashCode();
        hashCode = 31 * hashCode + relationship().hashCode();
        hashCode = 31 * hashCode + relationshipGroup().hashCode();
        hashCode = 31 * hashCode + property().hashCode();
        hashCode = 31 * hashCode + labelToken().hashCode();
        hashCode = 31 * hashCode + relationshipTypeToken().hashCode();
        hashCode = 31 * hashCode + propertyKeyToken().hashCode();
        hashCode = 31 * hashCode + dynamic().hashCode();
        return hashCode;
    }

    @Override
    public String toString()
    {
        return "RecordFormat:" + getClass().getSimpleName() + "[" + storeVersion() + "]";
    }

    @Override
    public boolean hasStore( StoreType store )
    {
        return true;
    }

    @Override
    public boolean hasCapability( Capability capability )
    {
        return contains( capabilities(), capability );
    }

    @Override
    public boolean hasSameCapabilities( RecordFormats other, int types )
    {
        Capability[] myFormatCapabilities =
                filter( this.capabilities(), capability -> capability.isType( types ) );
        Capability[] otherFormatCapabilities =
                filter( other.capabilities(), capability -> capability.isType( types ) );
        return containsAll( myFormatCapabilities, otherFormatCapabilities ) &&
                containsAll( otherFormatCapabilities, myFormatCapabilities );
    }
}
