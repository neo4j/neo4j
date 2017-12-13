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
package org.neo4j.kernel.impl.store.format;

import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.store.format.standard.MetaDataRecordFormat;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;

import static java.util.stream.Collectors.toSet;
import static org.neo4j.helpers.ArrayUtil.contains;

/**
 * Base class for simpler implementation of {@link RecordFormats}.
 */
public abstract class BaseRecordFormats implements RecordFormats
{
    private final int generation;
    private final Capability[] capabilities;
    private final String storeVersion;
    private final String introductionVersion;

    protected BaseRecordFormats( String storeVersion, String introductionVersion, int generation,
            Capability... capabilities )
    {
        this.storeVersion = storeVersion;
        this.generation = generation;
        this.capabilities = capabilities;
        this.introductionVersion = introductionVersion;
    }

    @Override
    public String storeVersion()
    {
        return storeVersion;
    }

    @Override
    public String introductionVersion()
    {
        return introductionVersion;
    }

    @Override
    public RecordFormat<MetaDataRecord> metaData()
    {
        return new MetaDataRecordFormat();
    }

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
    public int generation()
    {
        return generation;
    }

    @Override
    public Capability[] capabilities()
    {
        return capabilities;
    }

    @Override
    public boolean hasCapability( Capability capability )
    {
        return contains( capabilities(), capability );
    }

    public static boolean hasSameCapabilities( RecordFormats one, RecordFormats other, CapabilityType type )
    {
        Set<Capability> myFormatCapabilities = Stream.of( one.capabilities() )
                .filter( capability -> capability.isType( type ) ).collect( toSet() );
        Set<Capability> otherFormatCapabilities = Stream.of( other.capabilities() )
                .filter( capability -> capability.isType( type ) ).collect( toSet() );

        return myFormatCapabilities.equals( otherFormatCapabilities );
    }

    @Override
    public boolean hasSameCapabilities( RecordFormats other, CapabilityType type )
    {
        return hasSameCapabilities( this, other, type );
    }
}
