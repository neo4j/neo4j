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

import org.neo4j.hashing.HashFunction;

/**
 * Represents a stored schema rule.
 */
public interface SchemaRule extends SchemaDescriptorSupplier
{
    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    static String sanitiseName( Optional<String> name )
    {
        if ( name.isPresent() )
        {
            return sanitiseName( name.get() );
        }
        throw new IllegalArgumentException( "Schema rules must have names." );
    }

    static String sanitiseName( String name )
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "Schema rule name cannot be null." );
        }
        name = name.trim();
        if ( name.isEmpty() || name.isBlank() )
        {
            throw new IllegalArgumentException( "Schema rule name cannot be the empty string or only contain whitespace." );
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
                else if ( ch == '`' )
                {
                    throw new IllegalArgumentException( "Schema rule names are not allowed to contain back-tick characters: '" + name + "'." );
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
     * Generate a <em>deterministic</em> name for the given {@link SchemaDescriptorSupplier}.
     *
     * Only {@link SchemaRule} implementations, and {@link IndexPrototype}, are supported arguments for the schema descriptor supplier.
     *
     * @param rule The {@link SchemaDescriptorSupplier} to generate a name for.
     * @param entityTokenNames The resolved names of the schema entity tokens, that is, label names or relationship type names.
     * @param propertyNames The resolved property key names.
     * @return A name.
     */
    static String generateName( SchemaDescriptorSupplier rule, String[] entityTokenNames, String[] propertyNames )
    {
        // NOTE to future maintainers: You probably want to avoid touching this function.
        // Last time this was changed, we had some 400+ tests to update.
        HashFunction hf = HashFunction.incrementalXXH64();
        long key = hf.initialise( Boolean.hashCode( rule instanceof ConstraintDescriptor ) );
        key = hf.update( key, rule.schema().entityType().ordinal() );
        key = hf.update( key, rule.schema().propertySchemaType().ordinal() );
        key = hf.updateWithArray( key, entityTokenNames, String::hashCode );
        key = hf.updateWithArray( key, propertyNames, String::hashCode );

        if ( rule instanceof IndexRef<?> )
        {
            IndexRef<?> indexRef = (IndexRef<?>) rule;
            key = hf.update( key, indexRef.getIndexType().ordinal() );
            key = hf.update( key, Boolean.hashCode( indexRef.isUnique() ) );
            return String.format( "index_%x", hf.toInt( hf.finalise( key ) ) );
        }
        if ( rule instanceof ConstraintDescriptor )
        {
            ConstraintDescriptor constraint = (ConstraintDescriptor) rule;
            key = hf.update( key, constraint.type().ordinal() );
            return String.format( "constraint_%x", hf.toInt( hf.finalise( key ) ) );
        }
        throw new IllegalArgumentException( "Don't know how to generate a name for this SchemaDescriptorSupplier implementation: " + rule + "." );
    }

    /**
     * The persistence id for this rule.
     */
    long getId();

    /**
     * @return The (possibly user supplied) name of this schema rule.
     */
    String getName();

    /**
     * Produce a copy of this schema rule, that has the given name.
     * If the given name is {@code null}, then this schema rule is returned unchanged.
     * @param name The name of the new schema rule.
     * @return a modified copy of this schema rule.
     * @throws IllegalArgumentException if the given name is not {@code null}, and it fails the sanitise check.
     */
    SchemaRule withName( String name );
}
