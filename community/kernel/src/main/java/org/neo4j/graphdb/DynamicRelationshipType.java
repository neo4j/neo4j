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
package org.neo4j.graphdb;

/**
 * A dynamically instantiated and named {@link RelationshipType}. This class is
 * a convenience implementation of <code>RelationshipType</code> that is
 * typically used when relationship types are created and named after a
 * condition that can only be detected at runtime.
 * <p>
 * If all relationship types are known at compile time, it's better to use the
 * relationship type enum idiom as outlined in {@link RelationshipType}.
 * <p>
 * It's very important to note that a relationship type is uniquely identified
 * by its name, not by any particular instance that implements this interface.
 * This means that the proper way to check if two relationship types are equal
 * is by invoking <code>equals()</code> on their {@link #name() names}, NOT by
 * using Java's identity operator (<code>==</code>) or <code>equals()</code> on
 * the relationship type instances. A consequence of this is that you can NOT
 * use relationship types in hashed collections such as
 * {@link java.util.HashMap HashMap} and {@link java.util.HashSet HashSet}.
 * <p>
 * However, you usually want to check whether a specific relationship
 * <i>instance</i> is of a certain type. That is best achieved with the
 * {@link Relationship#isType Relationship.isType} method, such as:
 * 
 * <pre>
 * <code>
 * {@link RelationshipType} type = DynamicRelationshipType.{@link #withName(String) withName}( "myname" );
 * if ( rel.{@link Relationship#isType(RelationshipType) isType}( type ) )
 * {
 *     ...
 * }
 * </code>
 * </pre>
 */
public final class DynamicRelationshipType implements RelationshipType
{
    private final String name;

    private DynamicRelationshipType( final String name )
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "A relationship type cannot "
                                                + "have a null name" );
        }
        this.name = name;
    }

    /**
     * Instantiates a new DynamicRelationshipType with the given name.
     * There's more information regarding relationship types over at
     * {@link RelationshipType}.
     * 
     * @param name the name of the dynamic relationship type
     * @return a DynamicRelationshipType with the given name
     * @throws IllegalArgumentException if name is <code>null</code>
     */
    public static DynamicRelationshipType withName( final String name )
    {
        return new DynamicRelationshipType( name );
    }

    /**
     * Returns the name of this relationship type. The name uniquely identifies
     * a relationship type, i.e. two different RelationshipType instances with
     * different object identifiers (and possibly even different classes) are
     * semantically equivalent if they have {@link String#equals(Object) equal}
     * names.
     * 
     * @return the name of the relationship type
     */
    public String name()
    {
        return this.name;
    }

    /**
     * Returns a string representation of this dynamic relationship type.
     * 
     * @return a string representation of this dynamic relationship type
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return this.name;
    }

    /**
     * Implements the identity-based equals defined by {@link Object
     * java.lang.Object}. This means that this dynamic relationship type
     * instance will NOT be equal to other relationship types with the same
     * name. As outlined in the documentation for {@link RelationshipType
     * RelationshipType}, the proper way to check for equivalence between two
     * relationship types is to compare their {@link RelationshipType#name()
     * names}.
     * 
     * @return <code>true</code> if <code>other</code> is the same instance as
     *         this dynamic relationship type, <code>false</code> otherwise
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals( final Object other )
    {
        return super.equals( other );
    }

    /**
     * Implements the default hash function as defined by {@link Object
     * java.lang.Object}. This means that if you put a dynamic relationship
     * instance into a hash-based collection, it most likely will NOT behave as
     * you expect. Please see the documentation of {@link #equals(Object)
     * equals} and the {@link DynamicRelationshipType class documentation} for
     * more details.
     * 
     * @return a hash code value for this dynamic relationship type instance
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return super.hashCode();
    }
}
