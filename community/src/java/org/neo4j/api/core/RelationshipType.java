package org.neo4j.api.core;

/**
 * Represents a relationship's type, which is a mandatory property on all
 * relationships that is used for navigating the node space. RelationshipType is
 * in particular a key part
 *  of the {@link Traverser traverser framework} but it's also used in
 * various {@link Node#getRelationships() relationship operations} on Node.
 * <p>
 * Relationship types are declared by the client and can be handled either
 * dynamically or statically in a Neo-based application. Internally,
 * relationship types are dynamic. This means that every time a client invokes
 * {@link Node#createRelationshipTo(Node,RelationshipType)
 * node.createRelationshipTo(anotherNode, newRelType)} and passes in a new
 * relationship type then the new type will be transparently created.
 * <p>
 * However, in case the application does not need to dynamically create
 * relationship types (most don't), then it's nice to have the compile-time
 * benefits of a static set of relationship types. Fortunately, RelationshipType
 * is designed to work well with Java 5 enums. This means that it's very
 * easy to define a set of valid relationship types by declaring an enum that
 * implements RelationshipType and then reuse that across the application.
 * For example, here's how you would define an enum to hold all your
 * relationship types:
 * <code><pre>
 * enum MyRelationshipTypes implements RelationshipType
 * {
 *     CONTAINED_IN, KNOWS
 * }</pre></code>
 * 
 * Then later, it's as easy to use as:
 * <code><pre>
 * node.createRelationshipTo( anotherNode, MyRelationshipTypes.KNOWS );
 * 
 * for ( Relationship rel : node.getRelationships( MyRelationshipTypes.KNOWS ) )
 * {
 *     // ...
 * }
 * </pre></code>
 * 
 * Please note that in early 1.0 betas, you were required to supply an
 * enum of RelationshipTypes to the {@link EmbeddedNeo} constructor, or register
 * valid relationship types. This is no longer needed.
 * <p>
 * It's very important to note that a relationship type is uniquely
 * identified by its name, not by any particular instance that implements
 * this interface. This means that the proper way to check if two relationship
 * types are equal is by invoking <code>equals()</code> on their {@link #name
 * names}, NOT by using Java's identity operator (<code>==</code>). However,
 * you usually want to check whether a specific relationship <i>instance</i>
 * is of a certain type and that is best done with the
 * {@link Relationship#isType Relationship.isType} method, such as:
 * <code><pre> if ( rel.isType( MyRelationshipTypes.CONTAINED_IN ) )
 * {
 *     ...
 * }</pre></code> 
 */
public interface RelationshipType
{
	/**
	 * Returns the name of the relationship type. The name uniquely identifies
	 * a relationship type, i.e. two different RelationshipType implementations
	 * with different object identifies (and possibly even different classes)
	 * are semantically equivalent if they have {@link String#equals(Object)
	 * equal} names.
	 * @return the name of the relationship type
	 */
	public String name();
}
