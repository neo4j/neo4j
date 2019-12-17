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

import java.util.StringJoiner;
import java.util.function.IntFunction;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.token.api.TokenIdPrettyPrinter;

import static org.neo4j.common.EntityType.NODE;
import static org.neo4j.common.EntityType.RELATIONSHIP;

public final class SchemaUserDescription
{
    private SchemaUserDescription()
    {
    }

    static String forSchema( TokenNameLookup tokenNameLookup, EntityType entityType, int[] entityTokens, int[] propertyKeyIds )
    {
        String prefix = entityType == RELATIONSHIP ? "-[" : "";
        String suffix = entityType == RELATIONSHIP ? "]-" : "";
        IntFunction<String> lookup = entityType == NODE ? tokenNameLookup::labelGetName : tokenNameLookup::relationshipTypeGetName;
        return prefix + TokenIdPrettyPrinter.niceEntityLabels( lookup, entityTokens ) +
                TokenIdPrettyPrinter.niceProperties( tokenNameLookup, propertyKeyIds ) + suffix;
    }

    static String forPrototype( TokenNameLookup tokenNameLookup, String name, boolean isUnique, IndexType indexType,
            SchemaDescriptor schema, IndexProviderDescriptor indexProvider )
    {
        StringJoiner joiner = new StringJoiner( ", ", "Index( ", " )" );
        addPrototypeParams( tokenNameLookup, name, isUnique, indexType, schema, indexProvider, joiner );
        return joiner.toString();
    }

    static String forIndex( TokenNameLookup tokenNameLookup, long id, String name, boolean isUnique, IndexType indexType,
            SchemaDescriptor schema, IndexProviderDescriptor indexProvider )
    {
        StringJoiner joiner = new StringJoiner( ", ", "Index( ", " )" );
        joiner.add( Long.toString( id ) );
        addPrototypeParams( tokenNameLookup, name, isUnique, indexType, schema, indexProvider, joiner );
        return joiner.toString();
    }

    public static String forConstraint( TokenNameLookup tokenNameLookup, long id, String name, ConstraintType type, SchemaDescriptor schema, Long ownedIndex )
    {
        StringJoiner joiner = new StringJoiner( ", ", "Constraint( ", " )" );
        joiner.add( Long.toString( id ) );
        maybeAddName( name, joiner );
        joiner.add( typeName( type, schema.entityType() ) );
        joiner.add( schema.userDescription( tokenNameLookup ) );
        if ( ownedIndex != null )
        {
            joiner.add( "ownedIndex=" + ownedIndex );
        }
        return joiner.toString();
    }

    private static String typeName( ConstraintType type, EntityType entityType )
    {
        switch ( type )
        {
        case EXISTS: return entityType.name() + " PROPERTY EXISTENCE";
        case UNIQUE: return "UNIQUENESS";
        case UNIQUE_EXISTS: return "NODE KEY";
        default: return type.name();
        }
    }

    private static void maybeAddName( String name, StringJoiner joiner )
    {
        if ( name != null )
        {
            joiner.add( "'" + name + "'" );
        }
    }

    private static void addPrototypeParams( TokenNameLookup tokenNameLookup, String name, boolean isUnique, IndexType indexType, SchemaDescriptor schema,
            IndexProviderDescriptor indexProvider, StringJoiner joiner )
    {
        maybeAddName( name, joiner );
        joiner.add( (isUnique ? "UNIQUE" : "GENERAL") + " " + indexType );
        joiner.add( schema.userDescription( tokenNameLookup ) );
        joiner.add( indexProvider.name() );
    }

    private static String nullableName( String name )
    {
        return name == null ? "" : "'" + name + "', ";
    }
}
