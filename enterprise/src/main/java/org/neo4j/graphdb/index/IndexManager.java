package org.neo4j.graphdb.index;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * A one stop shop for creating and retrieving {@link Index}s for {@link Node}s
 * and {@link Relationship}s. An {@link IndexManager} is paired with a
 * {@link GraphDatabaseService} via {@link GraphDatabaseService#index()} so that
 * you can reach your indexes from your graph database.
 */
public interface IndexManager
{
    /**
     * Returns whether or not there exists a node index with the name
     * {@code indexName}. Indexes are created when needed in calls to
     * {@link #forNodes(String)} and {@link #forNodes(String, Map)}.
     * @param indexName the name of the index to check.
     * @return whether or not there exists a node index with the name
     * {@code indexName}.
     */
    boolean existsForNodes( String indexName );
    
    /**
     * Returns an {@link Index} for {@link Node}s with the name {@code indexName}.
     * If such an index doesn't exist it will be created with default configuration.
     * 
     * @param indexName the name of the node index.
     * @return the {@link Index} corresponding to the {@code indexName}.
     */
    Index<Node> forNodes( String indexName );

    /**
     * Returns an {@link Index} for {@link Node}s with the name {@code indexName}.
     * If the index exists it will be returned if the provider and customConfiguration
     * matches, else an {@link IllegalArgumentException} will be thrown.
     * If the index doesn't exist it will be created with the given
     * provider (specifies the type of index, f.ex. Lucene. See {@link IndexProvider})
     * and customConfiguration.
     * 
     * @param indexName the name of the index to create.
     * @param customConfiguration extra configuration for the index being created.
     * Use the <bold>provider</bold> key provider to control which index implementation,
     * i.e. the {@link IndexProvider} to use for this index if it's created. The
     * value represents the service name corresponding to the {@link IndexProvider}.
     * Other options can f.ex. say that the index will be a fulltext index, that it should
     * be case insensitive. The parameters given here are not generic parameters,
     * but instead interpreted by the implementation represented by the provider.
     */
    Index<Node> forNodes( String indexName, Map<String, String> customConfiguration );
    
    /**
     * Returns whether or not there exists a relationship index with the name
     * {@code indexName}. Indexes are created when needed in calls to
     * {@link #forRelationships(String)} and {@link #forRelationships(String, Map)}.
     * @param indexName the name of the index to check.
     * @return whether or not there exists a relationship index with the name
     * {@code indexName}.
     */
    boolean existsForRelationships( String indexName );
    
    /**
     * Returns an {@link Index} for {@link Relationship}s with the name {@code indexName}.
     * If such an index doesn't exist it will be created with default configuration.
     * 
     * @param indexName the name of the node index.
     * @return the {@link Index} corresponding to the {@code indexName}.
     */
    RelationshipIndex forRelationships( String indexName );

    /**
     * Returns an {@link Index} for {@link Relationship}s with the name {@code indexName}.
     * If the index exists it will be returned if the provider and customConfiguration
     * matches, else an {@link IllegalArgumentException} will be thrown.
     * If the index doesn't exist it will be created with the given
     * provider (specifies the type of index, f.ex. Lucene. See {@link IndexProvider})
     * and customConfiguration.
     * 
     * @param indexName the name of the index to create.
     * @param customConfiguration extra configuration for the index being created.
     * Use the <bold>provider</bold> key provider to control which index implementation,
     * i.e. the {@link IndexProvider} to use for this index if it's created. The
     * value represents the service name corresponding to the {@link IndexProvider}.
     * Other options can f.ex. say that the index will be a fulltext index, that it should
     * be case insensitive. The parameters given here are not generic parameters,
     * but instead interpreted by the implementation represented by the provider.
     */
    RelationshipIndex forRelationships( String indexName, Map<String, String> customConfiguration );
}
