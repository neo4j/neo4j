package org.neo4j.api.core;

/**
 * A dynamically instantiated and named {@link RelationshipType}. This class is
 * a convenience implementation of <code>RelationshipType</code> that is
 * typically used when relationship types are created and named after a
 * condition that can only be detected at runtime.
 * <p>
 * If all relationship types are
 * known at compile time, it's better to use the relationship type enum idiom
 * as outlined in {@link RelationshipType}.
 * <p>
 * It's very important to note that a relationship type is uniquely identified
 * by its name, not by any particular instance that implements this interface.
 * This means that the proper way to check if two relationship types are equal
 * is by invoking <code>equals()</code> on their {@link #name names}, NOT by
 * using Java's identity operator (<code>==</code>) or <code>equals()</code>
 * on the relationship type instances. A consequence of this is that you can NOT
 * use relationship types in hashed collections such as {@link java.util.HashMap
 * HashMap} and {@link java.util.HashSet HashSet}. 
 * <p>
 * However, you usually want to check whether a specific relationship
 * <i>instance</i> is of a certain type. That is best achieved with the
 * {@link Relationship#isType Relationship.isType} method, such as: <code><pre>
 * DynamicRelationshipType type = DynamicRelationshipType.withName( "myname" );
 * if ( rel.isType( type ) )
 * {
 *     ...
 * }
 * </pre></code>
 */
public final class DynamicRelationshipType implements RelationshipType
{
    private final String name;
    
    private DynamicRelationshipType( String name )
    {
        if ( name == null )
        {
            throw new IllegalArgumentException( "A relationship type cannot " +
            	"have a null name" );
        }
        this.name = name;
    }

    /**
     * Instantiates a new DynamicRelationshipType with the given name, without
     * creating it in the underlying storage. The relationship type is persisted
     * only when the first relationship of this type has been commited. 
     * @param name the name of the dynamic relationship type
     * @throws IllegalArgumentException if name is <code>null</code>
     */
    public static DynamicRelationshipType withName( String name )
    {
        return new DynamicRelationshipType( name );
    }

    /**
     * Returns the name of this relationship type. The name uniquely identifies
     * a relationship type, i.e. two different RelationshipType instances
     * with different object identifiers (and possibly even different classes)
     * are semantically equivalent if they have {@link String#equals(Object)
     * equal} names.
     * @return the name of the relationship type
     */
    public String name()
    {
        return this.name;
    }

    /**
     * Returns a string representation of this dynamic relationship type.
     * @return a string representation of this dynamic relationship type
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return "DynamicRelationshipType[" + this.name() + "]";
    }

    /**
     * Implements the identity-based equals defined by {@link Object
     * java.lang.Object}. This means that this dynamic relationship type
     * instance will NOT be equal to other relationship types with the same
     * name. As outlined in the documentation for {@link RelationshipType
     * RelationshipType}, the proper way to check for equivalence between two
     * relationship types is to compare their {@link RelationshipType#name()
     * names}. 
     * @return <code>true</code> if <code>other</code> is the same instance as
     * this dynamic relationship type, <code>false</code> otherwise
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals( Object other )
    {
        return super.equals( other );
    }

    /**
     * Implements the default hash function as defined by {@link Object
     * java.lang.Object}. This means that if you put a dynamic relationship
     * instance into a hash-based collection, it most likely will NOT behave
     * as you expect. Please see documentation of {@link #equals(Object) equals}
     * and the {@link DynamicRelationshipType class documentation} for more
     * details.
     * @return a hash code value for this dynamic relationship type instance
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return super.hashCode();
    }
}
