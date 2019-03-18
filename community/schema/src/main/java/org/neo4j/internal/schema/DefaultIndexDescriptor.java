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

import java.util.Optional;

import org.neo4j.common.TokenNameLookup;

import static java.lang.String.format;

/**
 * Default implementation of a {@link IndexDescriptor}. Mainly used as data carrier between the storage engine API and kernel.
 */
public class DefaultIndexDescriptor implements IndexDescriptor
{
    protected final Optional<String> name;
    private final SchemaDescriptor schema;
    private final String providerKey;
    private final String providerVersion;
    private final boolean isUnique;
    private final boolean isEventuallyConsistent;

    public DefaultIndexDescriptor(
            SchemaDescriptor schema,
            String providerKey,
            String providerVersion,
            Optional<String> name,
            boolean isUnique,
            boolean isEventuallyConsistent )
    {
        name.ifPresent( SchemaRule::checkName );
        this.schema = schema;
        this.providerKey = providerKey;
        this.providerVersion = providerVersion;
        this.name = name;
        this.isUnique = isUnique;
        this.isEventuallyConsistent = isEventuallyConsistent;
    }

    public DefaultIndexDescriptor( SchemaDescriptor schema, boolean isUnique )
    {
        this( schema, "Undecided", "0", Optional.empty(), isUnique, false );
    }

    @Override
    public boolean isUnique()
    {
        return isUnique;
    }

    @Override
    public boolean hasUserSuppliedName()
    {
        return name.isPresent();
    }

    @Override
    public String name()
    {
        return name.orElse( "Unnamed index" );
    }

    @Override
    public String userDescription( TokenNameLookup tokenNameLookup )
    {
        return format( "Index( %s, %s )", isUnique ? "UNIQUE" : "GENERAL", schema.userDescription( tokenNameLookup ) );
    }

    @Override
    public boolean isFulltextIndex()
    {
        return schema.isFulltextIndex();
    }

    @Override
    public boolean isEventuallyConsistent()
    {
        return isEventuallyConsistent;
    }

    @Override
    public String providerKey()
    {
        return providerKey;
    }

    @Override
    public String providerVersion()
    {
        return providerVersion;
    }

    @Override
    public SchemaDescriptor schema()
    {
        return schema;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( o instanceof IndexDescriptor )
        {
            IndexDescriptor that = (IndexDescriptor)o;
            return this.isUnique() == that.isUnique() && this.schema().equals( that.schema() );
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Boolean.hashCode( isUnique ) & schema.hashCode();
    }

    @Override
    public String toString()
    {
        return userDescription( TokenNameLookup.idTokenNameLookup );
    }

    public static Optional<String> optionalName( IndexDescriptor indexDescriptor )
    {
        return indexDescriptor.hasUserSuppliedName() ? Optional.of( indexDescriptor.name() ) : Optional.empty();
    }
}
