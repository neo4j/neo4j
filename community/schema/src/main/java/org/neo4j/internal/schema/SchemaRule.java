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

import java.util.Arrays;
import java.util.stream.Collectors;

import org.neo4j.common.EntityType;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Represents a stored schema rule.
 */
public interface SchemaRule extends SchemaDescriptorSupplier
{
    static String nameOrDefault( String name, String defaultName )
    {
        // Because of the difference how blank and trim works (whitespace check is totally different in those) we can't simply use isBlank here
        // and need to double check invoking trim that as result we do not have empty string there
        if ( isBlank( name ) || name.trim().isEmpty() )
        {
            name = defaultName;
        }
        return sanitiseName( name );
    }

    static String sanitiseName( String name )
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "Schema rule name cannot be null." );
        }
        name = name.trim();
        if ( name.isEmpty() )
        {
            throw new IllegalArgumentException( "Schema rule name cannot be the empty string." );
        }
        else
        {
            int length = name.length();
            for ( int i = 0; i < length; i++ )
            {
                char ch = name.charAt( i );
                if ( ch == '\0' )
                {
                    throw new IllegalArgumentException( "Schema rule names are not allowed to contain null-bytes: '" + name + "'." );
                }
            }
        }
        if ( ReservedSchemaRuleNames.contains( name ) )
        {
            throw new IllegalArgumentException( "The index name '" + name + "' is reserved, and cannot be used. " +
                    "The reserved names are " + ReservedSchemaRuleNames.getReservedNames() + "." );
        }
        return name;
    }

    /**
     * Generate a human friendly name for the given {@link IndexPrototype}, using the supplied arrays of resolved schema token names.
     * @param prototype The {@link IndexPrototype} to generate a name for.
     * @param entityTokenNames The resolved names of the schema entity tokens, that is, label names or relationship type names.
     * @param propertyNames The resolved property key names.
     * @return A suitable name.
     */
    static String generateName( IndexPrototype prototype, String[] entityTokenNames, String[] propertyNames )
    {
        SchemaDescriptor schema = prototype.schema();
        String indexType = schema.getIndexType() == IndexType.FULLTEXT ? "Full-Text Index" : prototype.isUnique() ? "Unique Index" : "Index";
        String entityPart = generateEntityNamePart( schema, entityTokenNames );
        return indexType + " on " + entityPart + " (" + String.join( ",", propertyNames ) + ")";
    }

    /**
     * Generate a human friendly name for the given {@link ConstraintDescriptor}, using the supplied arrays of resolved schema token names.
     * @param constraint The {@link ConstraintDescriptor} to generate a name for.
     * @param entityTokenNames The resolved names of the schema entity tokens, that is, label names or relationship type names.
     * @param propertyNames The resolved property key names.
     * @return A suitable name.
     */
    static String generateName( ConstraintDescriptor constraint, String[] entityTokenNames, String[] propertyNames )
    {
        SchemaDescriptor schema = constraint.schema();
        String constraintType;
        switch ( constraint.type() )
        {
        case EXISTS:
            constraintType = "Property existence constraint";
            break;
        case UNIQUE:
            constraintType = "Uniqueness constraint";
            break;
        case UNIQUE_EXISTS:
            if ( schema.entityType() != EntityType.NODE )
            {
                throw new IllegalArgumentException(
                        "Cannot describe " + ConstraintType.UNIQUE_EXISTS + " constraints for " + schema.entityType() + " entities." );
            }
            constraintType = "Node key constraint";
            break;
        default:
            throw new IllegalArgumentException( "Unknown ConstraintType: " + constraint.type() );
        }

        String entityPart = generateEntityNamePart( schema, entityTokenNames );
        return constraintType + " on " + entityPart + " (" + String.join( ",", propertyNames ) + ")";
    }

    private static String generateEntityNamePart( SchemaDescriptor schema, String[] entityTokenNames )
    {
        String entityPart;
        switch ( schema.entityType() )
        {
        case NODE:
            entityPart = Arrays.stream( entityTokenNames ).collect( Collectors.joining( ",:", ":", "" ) );
            break;
        case RELATIONSHIP:
            entityPart = Arrays.stream( entityTokenNames ).collect( Collectors.joining( "|:", "()-[:", "]-()" ) );
            break;
        default:
            throw new IllegalArgumentException( "Unknown EntityType: " + schema.entityType() + "." );
        }
        return entityPart;
    }

    /**
     * The persistence id for this rule.
     */
    long getId();

    /**
     * @return The (possibly user supplied) name of this schema rule.
     */
    String getName();
}
