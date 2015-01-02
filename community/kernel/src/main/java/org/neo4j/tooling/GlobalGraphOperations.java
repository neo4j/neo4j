/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.tooling;

import java.util.Iterator;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.FunctionFromPrimitiveLong;
import org.neo4j.helpers.collection.ResourceClosingIterator;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.api.operations.KeyReadOperations;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.core.Token;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.map;
import static org.neo4j.helpers.collection.IteratorUtil.emptyIterator;

/**
 * A tool for doing global operations, for example {@link #getAllNodes()}.
 */
public class GlobalGraphOperations
{
    private final NodeManager nodeManager;
    private final ThreadToStatementContextBridge statementCtxProvider;

    private GlobalGraphOperations( GraphDatabaseService db )
    {
        GraphDatabaseAPI dbApi = (GraphDatabaseAPI) db;
        DependencyResolver resolver = dbApi.getDependencyResolver();
        this.nodeManager = resolver.resolveDependency( NodeManager.class );
        this.statementCtxProvider = resolver.resolveDependency( ThreadToStatementContextBridge.class );
    }

    /**
     * Get a {@link GlobalGraphOperations} for the given {@code db}.
     *
     * @param db the {@link GraphDatabaseService} to get global operations for.
     * @return {@link GlobalGraphOperations} for the given {@code db}.
     */
    public static GlobalGraphOperations at( GraphDatabaseService db )
    {
        return new GlobalGraphOperations( db );
    }

    /**
     * Returns all nodes in the graph.
     *
     * @return all nodes in the graph.
     */
    public Iterable<Node> getAllNodes()
    {
        assertInTransaction();
        return new Iterable<Node>()
        {
            @Override
            public Iterator<Node> iterator()
            {
                return nodeManager.getAllNodes();
            }
        };
    }

    /**
     * Returns all relationships in the graph.
     *
     * @return all relationships in the graph.
     */
    public Iterable<Relationship> getAllRelationships()
    {
        assertInTransaction();
        return new Iterable<Relationship>()
        {
            @Override
            public Iterator<Relationship> iterator()
            {
                return nodeManager.getAllRelationships();
            }
        };
    }

    /**
     * Returns all relationship types currently in the underlying store. Relationship types are
     * added to the underlying store the first time they are used in a successfully committed
     * {@link Node#createRelationshipTo node.createRelationshipTo(...)}. Note that this method is
     * guaranteed to return all known relationship types, but it does not guarantee that it won't
     * return <i>more</i> than that (e.g. it can return "historic" relationship types that no longer
     * have any relationships in the graph).
     *
     * @return all relationship types in the underlying store
     */
    public Iterable<RelationshipType> getAllRelationshipTypes()
    {
        assertInTransaction();
        return nodeManager.getRelationshipTypes();
    }

    /**
     * Returns all labels currently in the underlying store. Labels are added to the store the first
     * they are used. This method guarantees that it will return all labels currently in use. However,
     * it may also return <i>more</i> than that (e.g. it can return "historic" labels that are no longer used).
     *
     * Please take care that the returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all labels in the underlying store.
     */
    public ResourceIterable<Label> getAllLabels()
    {
        assertInTransaction();
        return new ResourceIterable<Label>()
        {
            @Override
            public ResourceIterator<Label> iterator()
            {
                Statement statement = statementCtxProvider.instance();
                return ResourceClosingIterator.newResourceIterator( statement, map( new Function<Token, Label>()
                        {

                            @Override
                            public Label apply( Token labelToken )
                            {
                                return label( labelToken.name() );
                            }
                        }, statement.readOperations().labelsGetAllTokens() ) );
            }
        };
    }

    /**
     * Returns all property keys currently in the underlying store. This method guarantees that it will return all
     * property keys currently in use. However, it may also return <i>more</i> than that (e.g. it can return "historic"
     * labels that are no longer used).
     *
     * Please take care that the returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @return all property keys in the underlying store.
     */
    public ResourceIterable<String> getAllPropertyKeys()
    {
        assertInTransaction();
        return new ResourceIterable<String>()
        {
            @Override
            public ResourceIterator<String> iterator()
            {
                Statement statement = statementCtxProvider.instance();
                return ResourceClosingIterator.newResourceIterator( statement, map( new Function<Token, String>() {

                            @Override
                            public String apply( Token propertyToken )
                            {
                                return propertyToken.name();
                            }
                        }, statement.readOperations().propertyKeyGetAllTokens() ) );
            }
        };
    }

    /**
     * Returns all {@link Node nodes} with a specific {@link Label label}.
     *
     * Please take care that the returned {@link ResourceIterable} is closed correctly and as soon as possible
     * inside your transaction to avoid potential blocking of write operations.
     *
     * @param label the {@link Label} to return nodes for.
     * @return {@link Iterable} containing nodes with a specific label.
     */
    public ResourceIterable<Node> getAllNodesWithLabel( final Label label )
    {
        assertInTransaction();
        return new ResourceIterable<Node>()
        {
            @Override
            public ResourceIterator<Node> iterator()
            {
                return allNodesWithLabel( label.name() );
            }
        };
    }

    private ResourceIterator<Node> allNodesWithLabel( String label )
    {
        Statement statement = statementCtxProvider.instance();

        int labelId = statement.readOperations().labelGetForName( label );
        if ( labelId == KeyReadOperations.NO_SUCH_LABEL )
        {
            statement.close();
            return emptyIterator();
        }

        final PrimitiveLongIterator nodeIds = statement.readOperations().nodesGetForLabel( labelId );
        return ResourceClosingIterator.newResourceIterator( statement, map( new FunctionFromPrimitiveLong<Node>()
        {
            @Override
            public Node apply( long nodeId )
            {
                return nodeManager.getNodeById( nodeId );
            }
        }, nodeIds ) );
    }

    private void assertInTransaction()
    {
        statementCtxProvider.assertInTransaction();
    }
}
