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
 * using Java's identity operator (<code>==</code>). However, you usually
 * want to check whether a specific relationship <i>instance</i> is of a
 * certain type. That is best achieved with the
 * {@link Relationship#isType Relationship.isType} method, such as: <code><pre>
 * DynamicRelationshipType type = DynamicRelationshipType.withName( "a-name" );
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

    @Override
    public String toString()
    {
        return "DynamicRelationshipType[" + this.name() + "]";
    }
    
    /**
     * Instantiates a new DynamicRelationshipType with the given name, without
     * creating it in the underlying storage. The relationship type is created
     * only when the first relationship of this type has been commited. 
     * @param name the name of the dynamic relationship type
     * @throws IllegalArgumentException if name is <code>null</code>
     */
    public static DynamicRelationshipType withName( String name )
    {
        return new DynamicRelationshipType( name );
    }
}
