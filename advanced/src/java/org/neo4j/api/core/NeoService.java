package org.neo4j.api.core;

import java.io.Serializable;
import java.util.Map;

/**
 * The main access point to a running Neo instance. The only implementation
 * today is the {@link EmbeddedNeo EmbeddedNeo} class, which is used to
 * embed Neo intra-VM in an application. Typically, you would create an
 * <code>EmbeddedNeo</code> once and from then on access it through the
 * <code>NeoService</code> interface. For more information, see the class
 * documentation of {@link EmbeddedNeo EmbeddedNeo}.
 */
public interface NeoService
{
    /**
     * Creates a {@link Node}.
     * @return the created node.
     */
    public Node createNode();

    /**
     * Looks up a node by id.
     * @param id the id of the node 
     * @return the node with id <code>id</code> if found
     * @throws RuntimeException if not found
     */
    public Node getNodeById( long id );

    /**
     * Returns the reference node.
     * @return the reference node
     * @throws RuntimeException if unable to get the reference node
     */
    // TODO: Explain this concept
    public Node getReferenceNode();

    /**
     * Shuts down Neo. After this method has been invoked, it's invalid to
     * invoke any methods in the Neo API.
     */
    public void shutdown();
    
    /**
     * Enables remote shell access to this Neo instance, if the Neo4j
     * <code>shell</code> project is available on the classpath. This method
     * passed no configuration parameters and is identical to invoking
     * {@link #enableRemoteShell(Map) enableRemoteShell( null )}.
     * @return <code>true</code> if the shell has been enabled,
     * <code>false</code> otherwise (<code>false</code> usually indicates that
     * the <code>shell</code> jar dependency is not on the classpath)
     */
    public boolean enableRemoteShell();

    /**
     * Enables remote shell access to this Neo instance, if the Neo4j
     * <code>shell</code> project is available on the classpath. This will
     * publish a shell access interface on an RMI registry on localhost (with
     * configurable port and RMI binding name). It can be accessed by a
     * client that implements <code>org.neo4j.util.shell.ShellClient</code>
     * from the Neo4J <code>shell</code> project. Typically, the
     * <code>neoshell</code> binary package is used (see
     * <a href="http://neo4j.org/download">neo4j.org/download</a>).
     * <p>
     * The shell is parameterized by a map of properties passed in to this
     * method. Currently, two properties are used:
     * <ul>
     *	<li><code>port</code>, an {@link Integer} describing the port of the RMI
     * registry where the Neo shell will be bound, defaults to <code>1337</code>
     *	<li><code>name</code>, the {@link String} under which the Neo shell will
     * be bound in the RMI registry, defaults to <code>neoshell</code>
     * </ul>
     * @param initialProperties a set of properties that will be used to
     * configure the remote shell, or <code>null</code> if the default
     * properties should be used
     * @return <code>true</code> if the shell has been enabled,
     * <code>false</code> otherwise (<code>false</code> usually indicates that
     * the <code>shell</code> jar dependency is not on the classpath)
     * @throws ClassCastException if the shell library is available, but one
     * (or more) of the configuration properties have an unexpected type
     * @throws IllegalStateException if the shell library is available, but
     * the remote shell can't be enabled anyway
     */
    public boolean enableRemoteShell(
        Map<String, Serializable> initialProperties );

    public void registerEnumRelationshipTypes(
        Class<? extends RelationshipType> relationshipTypes );

    public Iterable<RelationshipType> getRelationshipTypes();

    public RelationshipType getRelationshipType( String name );

    public boolean hasRelationshipType( String name );

    public RelationshipType createAndRegisterRelationshipType( String name );

    public RelationshipType registerRelationshipType( String name );

    public void registerRelationshipTypes( Iterable<RelationshipType> types );

    public void registerRelationshipTypes( RelationshipType[] types );
}