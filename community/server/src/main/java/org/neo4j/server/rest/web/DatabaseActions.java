/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.rest.web;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Sort;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.ReadableIndex;
import org.neo4j.graphdb.index.ReadableRelationshipIndex;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.TransactionBuilder;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.server.database.Database;
import org.neo4j.server.rest.domain.EndNodeNotFoundException;
import org.neo4j.server.rest.domain.RelationshipExpanderBuilder;
import org.neo4j.server.rest.domain.StartNodeNotFoundException;
import org.neo4j.server.rest.domain.TraversalDescriptionBuilder;
import org.neo4j.server.rest.domain.TraverserReturnType;
import org.neo4j.server.rest.paging.Lease;
import org.neo4j.server.rest.paging.LeaseManager;
import org.neo4j.server.rest.paging.PagedTraverser;
import org.neo4j.server.rest.repr.BadInputException;
import org.neo4j.server.rest.repr.DatabaseRepresentation;
import org.neo4j.server.rest.repr.IndexRepresentation;
import org.neo4j.server.rest.repr.IndexedEntityRepresentation;
import org.neo4j.server.rest.repr.ListRepresentation;
import org.neo4j.server.rest.repr.NodeIndexRepresentation;
import org.neo4j.server.rest.repr.NodeIndexRootRepresentation;
import org.neo4j.server.rest.repr.NodeRepresentation;
import org.neo4j.server.rest.repr.PathRepresentation;
import org.neo4j.server.rest.repr.PropertiesRepresentation;
import org.neo4j.server.rest.repr.RelationshipIndexRepresentation;
import org.neo4j.server.rest.repr.RelationshipIndexRootRepresentation;
import org.neo4j.server.rest.repr.RelationshipRepresentation;
import org.neo4j.server.rest.repr.Representation;
import org.neo4j.server.rest.repr.RepresentationType;
import org.neo4j.server.rest.repr.ScoredNodeRepresentation;
import org.neo4j.server.rest.repr.ScoredRelationshipRepresentation;
import org.neo4j.server.rest.repr.ValueRepresentation;
import org.neo4j.server.rest.repr.WeightedPathRepresentation;

public class DatabaseActions
{
    public static final String SCORE_ORDER = "score";
    public static final String RELEVANCE_ORDER = "relevance";
    public static final String INDEX_ORDER = "index";
    private final Database database;
    private final GraphDatabaseAPI graphDb;
    private final LeaseManager leases;
    private final ForceMode defaultForceMode;

    public DatabaseActions( Database database, LeaseManager leaseManager, ForceMode defaultForceMode )
    {
        this.leases = leaseManager;
        this.defaultForceMode = defaultForceMode;
        this.database = database;
        this.graphDb = database.getGraph();
    }

    public DatabaseActions forceMode( ForceMode forceMode )
    {
        return forceMode == defaultForceMode || forceMode == null ? this : new DatabaseActions( database, leases, forceMode );
    }

    private Node node( long id ) throws NodeNotFoundException
    {
        try
        {
            return graphDb.getNodeById( id );
        }
        catch ( NotFoundException e )
        {
            throw new NodeNotFoundException( String.format(
                    "Cannot find node with id [%d] in database.", id ) );
        }
    }

    private Relationship relationship( long id )
            throws RelationshipNotFoundException
    {
        try
        {
            return graphDb.getRelationshipById( id );
        }
        catch ( NotFoundException e )
        {
            throw new RelationshipNotFoundException();
        }
    }

    private <T extends PropertyContainer> T set( T entity,
            Map<String, Object> properties ) throws PropertyValueException
    {
        if ( properties != null )
        {
            for ( Map.Entry<String, Object> property : properties.entrySet() )
            {
                try
                {
                    entity.setProperty( property.getKey(),
                            property( property.getValue() ) );
                }
                catch ( IllegalArgumentException ex )
                {
                    throw new PropertyValueException( property.getKey(),
                            property.getValue() );
                }
            }
        }
        return entity;
    }

    private Object property( Object value )
    {
        if ( value instanceof Collection<?> )
        {
            Collection<?> collection = (Collection<?>) value;
            Object[] array = null;
            Iterator<?> objects = collection.iterator();
            for ( int i = 0; objects.hasNext(); i++ )
            {
                Object object = objects.next();
                if ( array == null )
                {
                    array = (Object[]) Array.newInstance( object.getClass(),
                            collection.size() );
                }
                array[i] = object;
            }
            return array;
        }
        else
        {
            return value;
        }
    }

    private <T extends PropertyContainer> T clear( T entity )
    {
        for ( String key : entity.getPropertyKeys() )
        {
            entity.removeProperty( key );
        }
        return entity;
    }

    // API

    public DatabaseRepresentation root()
    {
        return new DatabaseRepresentation( graphDb );
    }

    // Nodes

    public NodeRepresentation createNode( Map<String, Object> properties )
            throws PropertyValueException
    {
        final NodeRepresentation result;
        Transaction tx = beginTx();
        try
        {
            result = new NodeRepresentation( set( graphDb.createNode(),
                    properties ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return result;
    }

    private Transaction beginTx()
    {
        TransactionBuilder tx = graphDb.tx();
        if ( defaultForceMode == ForceMode.unforced ) tx = tx.unforced();
        return tx.begin();
    }

    public NodeRepresentation getNode( long nodeId )
            throws NodeNotFoundException
    {
        return new NodeRepresentation( node( nodeId ) );
    }

    public void deleteNode( long nodeId ) throws NodeNotFoundException,
            OperationFailureException
    {
        Node node = node( nodeId );
        Transaction tx = beginTx();
        try
        {
            node.delete();
            tx.success();
        }
        finally
        {
            try
            {
                tx.finish();
            }
            catch ( TransactionFailureException e )
            {
                throw new OperationFailureException(
                        String.format(
                                "The node with id %d cannot be deleted. Check that the node is orphaned before deletion.",
                                nodeId ) );
            }
        }
    }

    public NodeRepresentation getReferenceNode()
    {
        return new NodeRepresentation( graphDb.getReferenceNode() );
    }

    // Node properties

    public Representation getNodeProperty( long nodeId, String key )
            throws NodeNotFoundException, NoSuchPropertyException
    {
        Node node = node( nodeId );
        try
        {
            return PropertiesRepresentation.value( node.getProperty( key ) );
        }
        catch ( NotFoundException e )
        {
            throw new NoSuchPropertyException( node, key );
        }
    }

    public void setNodeProperty( long nodeId, String key, Object value )
            throws PropertyValueException, NodeNotFoundException
    {
        Node node = node( nodeId );
        value = property( value );
        Transaction tx = beginTx();
        try
        {
            node.setProperty( key, value );
            tx.success();
        }
        catch ( IllegalArgumentException e )
        {
            throw new PropertyValueException( key, value );
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeNodeProperty( long nodeId, String key )
            throws NodeNotFoundException, NoSuchPropertyException
    {
        Node node = node( nodeId );
        Transaction tx = beginTx();
        try
        {
            if ( node.removeProperty( key ) == null )
            {
                throw new NoSuchPropertyException( node, key );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public PropertiesRepresentation getAllNodeProperties( long nodeId )
            throws NodeNotFoundException
    {
        return new PropertiesRepresentation( node( nodeId ) );
    }

    public void setAllNodeProperties( long nodeId,
            Map<String, Object> properties ) throws PropertyValueException,
            NodeNotFoundException
    {
        Node node = node( nodeId );
        Transaction tx = beginTx();
        try
        {
            set( clear( node ), properties );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeAllNodeProperties( long nodeId )
            throws NodeNotFoundException
    {
        Node node = node( nodeId );
        Transaction tx = beginTx();
        try
        {
            clear( node );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public String[] getNodeIndexNames()
    {
        return graphDb.index().nodeIndexNames();
    }

    public String[] getRelationshipIndexNames()
    {
        return graphDb.index().relationshipIndexNames();
    }

    public IndexRepresentation createNodeIndex(
            Map<String, Object> indexSpecification )
    {
        final String indexName = (String) indexSpecification.get( "name" );
        
        assertIsLegalIndexName(indexName);
        
        if ( indexSpecification.containsKey( "config" ) )
        {

            @SuppressWarnings( "unchecked" )
            Map<String, String> config = (Map<String, String>) indexSpecification.get( "config" );
            graphDb.index().forNodes( indexName, config );

            return new NodeIndexRepresentation( indexName, config );
        }

        graphDb.index().forNodes( indexName );
        return new NodeIndexRepresentation( indexName,
                Collections.<String, String>emptyMap() );
    }

    public IndexRepresentation createRelationshipIndex(
            Map<String, Object> indexSpecification )
    {
        final String indexName = (String) indexSpecification.get( "name" );
        
        assertIsLegalIndexName(indexName);
        
        if ( indexSpecification.containsKey( "config" ) )
        {

            @SuppressWarnings( "unchecked" )
            Map<String, String> config = (Map<String, String>) indexSpecification.get( "config" );
            graphDb.index().forRelationships( indexName, config );

            return new RelationshipIndexRepresentation( indexName, config );
        }

        graphDb.index().forRelationships( indexName );
        return new RelationshipIndexRepresentation( indexName,
                Collections.<String, String>emptyMap() );
    }

    public void removeNodeIndex( String indexName )
    {
        if(!graphDb.index().existsForNodes(indexName)) {
            throw new NotFoundException("No node index named '" + indexName + "'.");
        }
        
        Index<Node> index = graphDb.index().forNodes( indexName );
        Transaction tx = beginTx();
        try
        {
            index.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeRelationshipIndex( String indexName )
    {
        if(!graphDb.index().existsForRelationships(indexName)) {
            throw new NotFoundException("No relationship index named '" + indexName + "'.");
        }
        
        Index<Relationship> index = graphDb.index().forRelationships( indexName );
        Transaction tx = beginTx();
        try
        {
            index.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public boolean nodeIsIndexed( String indexName, String key, Object value,
            long nodeId )
    {

        Index<Node> index = graphDb.index().forNodes( indexName );
        Transaction tx = beginTx();
        try
        {
            Node expectedNode = graphDb.getNodeById( nodeId );
            IndexHits<Node> hits = index.get( key, value );
            boolean contains = iterableContains( hits, expectedNode );
            tx.success();
            return contains;
        }
        finally
        {
            tx.finish();
        }
    }

    public boolean relationshipIsIndexed( String indexName, String key,
            Object value, long relationshipId )
    {

        Index<Relationship> index = graphDb.index().forRelationships( indexName );
        Transaction tx = beginTx();
        try
        {
            Relationship expectedNode = graphDb.getRelationshipById( relationshipId );
            IndexHits<Relationship> hits = index.get( key, value );
            boolean contains = iterableContains( hits, expectedNode );
            tx.success();
            return contains;
        }
        finally
        {
            tx.finish();
        }
    }

    private <T> boolean iterableContains( Iterable<T> iterable,
            T expectedElement )
    {
        for ( T possibleMatch : iterable )
        {
            if ( possibleMatch.equals( expectedElement ) ) return true;
        }
        return false;
    }

    public Representation isAutoIndexerEnabled(String type) {
        AutoIndexer<? extends PropertyContainer> index = getAutoIndexerForType(type);
        return ValueRepresentation.bool(index.isEnabled());
    }

    public void setAutoIndexerEnabled(String type, boolean enable) {
        AutoIndexer<? extends PropertyContainer> index = getAutoIndexerForType(type);
        index.setEnabled(enable);
    }

    private AutoIndexer<? extends PropertyContainer> getAutoIndexerForType(String type) {
        final IndexManager indexManager = graphDb.index();
        if ("node".equals(type)) {
            return indexManager.getNodeAutoIndexer();
        } else if ("relationship".equals(type)) {
            return indexManager.getRelationshipAutoIndexer();
        } else {
            throw new IllegalArgumentException("invalid type " + type);
        }
    }

    public Representation getAutoIndexedProperties(String type) {
        AutoIndexer<? extends PropertyContainer> indexer = getAutoIndexerForType(type);
        return ListRepresentation.string(indexer.getAutoIndexedProperties());
    }

    public void startAutoIndexingProperty(String type, String property) {
        AutoIndexer<? extends PropertyContainer> indexer = getAutoIndexerForType(type);
        indexer.startAutoIndexingProperty(property);
    }

    public void stopAutoIndexingProperty(String type, String property) {
        AutoIndexer<? extends PropertyContainer> indexer = getAutoIndexerForType(type);
        indexer.stopAutoIndexingProperty(property);
    }

    // Relationships

    public enum RelationshipDirection
    {
        all( Direction.BOTH ),
        in( Direction.INCOMING ),
        out( Direction.OUTGOING );
        final Direction internal;

        private RelationshipDirection( Direction internal )
        {
            this.internal = internal;
        }
    }

    public RelationshipRepresentation createRelationship( long startNodeId,
            long endNodeId, String type, Map<String, Object> properties )
            throws StartNodeNotFoundException, EndNodeNotFoundException,
            PropertyValueException
    {

        Node start, end;
        try
        {
            start = node( startNodeId );
        }
        catch ( NodeNotFoundException e )
        {
            throw new StartNodeNotFoundException();
        }
        try
        {
            end = node( endNodeId );
        }
        catch ( NodeNotFoundException e )
        {
            throw new EndNodeNotFoundException();
        }
        final RelationshipRepresentation result;
        Transaction tx = beginTx();
        try
        {
            result = new RelationshipRepresentation( set(
                    start.createRelationshipTo( end,
                            DynamicRelationshipType.withName( type ) ),
                    properties ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return result;
    }

    public RelationshipRepresentation getRelationship( long relationshipId )
            throws RelationshipNotFoundException
    {
        return new RelationshipRepresentation( relationship( relationshipId ) );
    }

    public void deleteRelationship( long relationshipId )
            throws RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        Transaction tx = beginTx();
        try
        {
            relationship.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @SuppressWarnings( "unchecked" )
    public ListRepresentation getNodeRelationships( long nodeId,
            RelationshipDirection direction, Collection<String> types )
            throws NodeNotFoundException
    {
        Node node = node( nodeId );
        Expander expander;
        if ( types.isEmpty() )
        {
            expander = Traversal.expanderForAllTypes( direction.internal );
        }
        else
        {
            expander = Traversal.emptyExpander();
            for ( String type : types )
            {
                expander = expander.add(
                        DynamicRelationshipType.withName( type ),
                        direction.internal );
            }
        }
        return RelationshipRepresentation.list( expander.expand( node ) );
    }

    // Relationship properties

    public PropertiesRepresentation getAllRelationshipProperties(
            long relationshipId ) throws RelationshipNotFoundException
    {
        return new PropertiesRepresentation( relationship( relationshipId ) );
    }

    public Representation getRelationshipProperty( long relationshipId,
            String key ) throws NoSuchPropertyException,
            RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        try
        {
            return PropertiesRepresentation.value( relationship.getProperty( key ) );
        }
        catch ( NotFoundException e )
        {
            throw new NoSuchPropertyException( relationship, key );
        }
    }

    public void setAllRelationshipProperties( long relationshipId,
            Map<String, Object> properties ) throws PropertyValueException,
            RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        Transaction tx = beginTx();
        try
        {
            set( clear( relationship ), properties );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void setRelationshipProperty( long relationshipId, String key,
            Object value ) throws PropertyValueException,
            RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        value = property( value );
        Transaction tx = beginTx();
        try
        {
            relationship.setProperty( key, value );
            tx.success();
        }
        catch ( IllegalArgumentException e )
        {
            throw new PropertyValueException( key, value );
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeAllRelationshipProperties( long relationshipId )
            throws RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        Transaction tx = beginTx();
        try
        {
            clear( relationship );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeRelationshipProperty( long relationshipId, String key )
            throws RelationshipNotFoundException, NoSuchPropertyException
    {
        Relationship relationship = relationship( relationshipId );
        Transaction tx = beginTx();
        try
        {
            if ( relationship.removeProperty( key ) == null )
            {
                throw new NoSuchPropertyException( relationship, key );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    // Index

    public enum IndexType
    {
        node( "index" )
        {
        },
        relationship( "index" )
        {
        };
        private final String pathPrefix;

        private IndexType( String pathPrefix )
        {
            this.pathPrefix = pathPrefix;
        }

        @SuppressWarnings( "boxing" )
        String path( String indexName, String key, String value, long id )
        {
            return String.format( "%s/%s/%s/%s/%s", pathPrefix, indexName, key,
                    value, id );
        }
    }

    public Representation nodeIndexRoot()
    {
        return new NodeIndexRootRepresentation( graphDb.index() );
    }

    public Representation relationshipIndexRoot()
    {
        return new RelationshipIndexRootRepresentation( graphDb.index() );
    }

    public IndexedEntityRepresentation addToRelationshipIndex(
            String indexName, String key, String value, long relationshipId )
    {
        Transaction tx = beginTx();
        try
        {
            Relationship relationship = graphDb.getRelationshipById( relationshipId );
            Index<Relationship> index = graphDb.index().forRelationships(
                    indexName );
            index.add( relationship, key, value );
            tx.success();
            return new IndexedEntityRepresentation( relationship, key, value,
                    new RelationshipIndexRepresentation( indexName,
                            Collections.<String, String>emptyMap() ) );
        }
        finally
        {
            tx.finish();
        }
    }

    public IndexedEntityRepresentation addToNodeIndex( String indexName,
            String key, String value, long nodeId )
    {
        Transaction tx = beginTx();
        try
        {
            Node node = graphDb.getNodeById( nodeId );
            Index<Node> index = graphDb.index().forNodes( indexName );
            index.add( node, key, value );
            tx.success();
            return new IndexedEntityRepresentation( node, key, value,
                    new NodeIndexRepresentation( indexName,
                            Collections.<String, String>emptyMap() ) );
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeFromNodeIndex( String indexName, String key,
            String value, long id )
    {
        Index<Node> index = graphDb.index().forNodes( indexName );
        Transaction tx = beginTx();
        try
        {
            index.remove( graphDb.getNodeById( id ), key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeFromNodeIndexNoValue( String indexName, String key,
            long id )
    {
        Index<Node> index = graphDb.index().forNodes( indexName );
        Transaction tx = beginTx();
        try
        {
            index.remove( graphDb.getNodeById( id ), key );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeFromNodeIndexNoKeyValue( String indexName, long id )
    {
        Index<Node> index = graphDb.index().forNodes( indexName );
        Transaction tx = beginTx();
        try
        {
            index.remove( graphDb.getNodeById( id ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeFromRelationshipIndex( String indexName, String key,
            String value, long id )
    {
        RelationshipIndex index = graphDb.index().forRelationships( indexName );
        Transaction tx = beginTx();
        try
        {
            index.remove( graphDb.getRelationshipById( id ), key, value );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeFromRelationshipIndexNoValue( String indexName,
            String key, long id )
    {
        RelationshipIndex index = graphDb.index().forRelationships( indexName );
        Transaction tx = beginTx();
        try
        {
            index.remove( graphDb.getRelationshipById( id ), key );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void removeFromRelationshipIndexNoKeyValue( String indexName, long id )
    {
        RelationshipIndex index = graphDb.index().forRelationships( indexName );
        Transaction tx = beginTx();
        try
        {
            index.remove( graphDb.getRelationshipById( id ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public IndexedEntityRepresentation getIndexedNode( String indexName,
            String key, String value, long id )
    {
        if ( !nodeIsIndexed( indexName, key, value, id ) )
            throw new NotFoundException();
        Node node = graphDb.getNodeById( id );
        return new IndexedEntityRepresentation( node, key, value,
                new NodeIndexRepresentation( indexName,
                        Collections.<String, String>emptyMap() ) );
    }

    public IndexedEntityRepresentation getIndexedRelationship(
            String indexName, String key, String value, long id )
    {
        if ( !relationshipIsIndexed( indexName, key, value, id ) )
            throw new NotFoundException();
        Relationship node = graphDb.getRelationshipById( id );
        return new IndexedEntityRepresentation( node, key, value,
                new RelationshipIndexRepresentation( indexName,
                        Collections.<String, String>emptyMap() ) );
    }

    public ListRepresentation getIndexedNodes( String indexName, final String key,
            final String value )
    {
        if ( !graphDb.index().existsForNodes( indexName ) )
            throw new NotFoundException();
        
            Index<Node> index = graphDb.index().forNodes( indexName );

            final IndexRepresentation indexRepresentation = new NodeIndexRepresentation(indexName);
            final IndexHits<Node> indexHits = index.get( key, value );

            final IterableWrapper<Representation, Node> results = new IterableWrapper<Representation, Node>(indexHits)
            {
                @Override
				protected Representation underlyingObjectToObject(Node node)
                {
                    return new IndexedEntityRepresentation( node, key, value, indexRepresentation);
                }
            };
            return new ListRepresentation( RepresentationType.NODE, results);
    }

    public ListRepresentation getIndexedNodesByQuery( String indexName,
            String query, String sort )
    {
        return getIndexedNodesByQuery( indexName, null, query, sort );
    }

    public ListRepresentation getIndexedNodesByQuery( String indexName,
            String key, String query, String sort )
    {
        if ( !graphDb.index().existsForNodes( indexName ) )
            throw new NotFoundException();
        
        if (query == null)
        {
            return toListNodeRepresentation();
        }
        Index<Node> index = graphDb.index().forNodes( indexName );

        IndexResultOrder order = getOrdering( sort );
        QueryContext queryCtx = order.updateQueryContext( new QueryContext(query));
        IndexHits<Node> result = index.query( key, queryCtx );
        return toListNodeRepresentation(result ,order);
    }

    private ListRepresentation toListNodeRepresentation()
    {
            return new ListRepresentation( RepresentationType.NODE, Collections.<Representation>emptyList());
    }

    private ListRepresentation toListNodeRepresentation(final IndexHits<Node> result, final IndexResultOrder order) {
        if (result == null)
        {
            return new ListRepresentation( RepresentationType.NODE, Collections.<Representation>emptyList());
        }
        final IterableWrapper<Representation, Node> results = new IterableWrapper<Representation, Node>(result)
        {
            @Override
            protected Representation underlyingObjectToObject(Node node)
            {
                final NodeRepresentation nodeRepresentation = new NodeRepresentation(node);
                if (order == null ) {
                    return nodeRepresentation;
                }
                return order.getRepresentationFor(nodeRepresentation, result.currentScore());
            }
        };
        return new ListRepresentation( RepresentationType.NODE, results);
    }
    private ListRepresentation toListRelationshipRepresentation()
    {
        return new ListRepresentation( RepresentationType.RELATIONSHIP, Collections.<Representation>emptyList());
    }

    private ListRepresentation toListRelationshipRepresentation(final IndexHits<Relationship> result, final IndexResultOrder order) {
        if (result == null)
        {
            return new ListRepresentation( RepresentationType.RELATIONSHIP, Collections.<Representation>emptyList());
        }
        final IterableWrapper<Representation, Relationship> results = new IterableWrapper<Representation, Relationship>(result)
        {
            @Override
            protected Representation underlyingObjectToObject(Relationship rel)
            {
                final RelationshipRepresentation relationshipRepresentation = new RelationshipRepresentation(rel);
                if (order != null )
                {
                    return order.getRepresentationFor(relationshipRepresentation, result.currentScore());
                }
                return relationshipRepresentation;
            }
        };
        return new ListRepresentation( RepresentationType.RELATIONSHIP, results);
    }

    public Pair<IndexedEntityRepresentation, Boolean> getOrCreateIndexedNode( String indexName, String key,
                                                                              String value, Long nodeOrNull,
                                                                              Map<String, Object> properties ) throws BadInputException, NodeNotFoundException
    {
        assertIsLegalIndexName(indexName);
        
        Transaction tx = beginTx();
        try
        {
            Node result;
            boolean created;
            if ( nodeOrNull != null )
            {
                if ( properties != null )
                {
                    throw new BadInputException( "Cannot specify properties for a new node, when a node to index is specified." );
                }
                Node node = node(nodeOrNull);
                result = graphDb.index().forNodes( indexName ).putIfAbsent( node, key, value );
                if ( ( created = ( result == null ) ) == true ) result = node;
            }
            else
            {
                UniqueNodeFactory factory = new UniqueNodeFactory( indexName, properties );
                result = factory.getOrCreate( key, value );
                created = factory.created;
            }
            tx.success();
            return Pair.of( new IndexedEntityRepresentation( result, key, value,
                        new NodeIndexRepresentation( indexName, Collections.<String, String>emptyMap() ) ), created);
        }
        finally
        {
            tx.finish();
        }
    }

    public Pair<IndexedEntityRepresentation, Boolean> getOrCreateIndexedRelationship( String indexName, String key,
                                                                                      String value, Long relationshipOrNull,
                                                                                      Long startNode, String type, Long endNode,
                                                                                      Map<String, Object> properties ) throws BadInputException, RelationshipNotFoundException, NodeNotFoundException
    {
        assertIsLegalIndexName(indexName);
        
        Transaction tx = beginTx();
        try
        {
            Relationship result;
            boolean created;
            if ( relationshipOrNull != null )
            {
                if ( startNode != null || type != null || endNode != null || properties != null )
                {
                    throw new BadInputException( "Either specify a relationship to index uniquely, or the means for creating it." );
                }
                Relationship relationship = relationship(relationshipOrNull);
                result = graphDb.index().forRelationships( indexName ).putIfAbsent( relationship, key, value );
                if ( ( created = ( result == null ) ) == true ) result = relationship;
            }
            else if ( startNode == null || type == null || endNode == null )
            {
                throw new BadInputException( "Either specify a relationship to index uniquely, or the means for creating it." );
            }
            else
            {
                UniqueRelationshipFactory factory = new UniqueRelationshipFactory( indexName, node(startNode), node(endNode), type, properties );
                result = factory.getOrCreate( key, value );
                created = factory.created;
            }
            tx.success();
            return Pair.of( new IndexedEntityRepresentation( result, key, value,
                        new RelationshipIndexRepresentation( indexName, Collections.<String, String>emptyMap() ) ),
                    created);
        }
        finally
        {
            tx.finish();
        }
    }

    private class UniqueRelationshipFactory extends UniqueFactory.UniqueRelationshipFactory
    {
        private final Node start, end;
        private final RelationshipType type;
        private final Map<String, Object> properties;
        boolean created;

        UniqueRelationshipFactory( String index, Node start, Node end, String type, Map<String, Object> properties )
        {
            super( graphDb, index );
            this.start = start;
            this.end = end;
            this.type = DynamicRelationshipType.withName( type );
            this.properties = properties;
        }
        @Override
        protected Relationship create( Map<String, Object> ignored )
        {
            return start.createRelationshipTo( end, type );
        }

        @Override
        protected void initialize( Relationship relationship, Map<String, Object> indexed )
        {
            for ( Map.Entry<String, Object> property : (properties == null ? indexed : properties).entrySet() )
            {
                relationship.setProperty( property.getKey(), property.getValue() );
            }
            this.created = true;
        }
    }

    private class UniqueNodeFactory extends UniqueFactory.UniqueNodeFactory
    {
        private final Map<String, Object> properties;
        boolean created;

        UniqueNodeFactory( String index, Map<String, Object> properties )
        {
            super( graphDb, index );
            this.properties = properties;
        }

        @Override
        protected void initialize( Node node, Map<String, Object> indexed )
        {
            for ( Map.Entry<String, Object> property : (properties == null ? indexed : properties).entrySet() )
            {
                node.setProperty( property.getKey(), property.getValue() );
            }
            this.created = true;
        }
    }

    public Representation getAutoIndexedNodes( String key, String value )
    {
        ReadableIndex<Node> index = graphDb.index().getNodeAutoIndexer().getAutoIndex();

        return toListNodeRepresentation(index.get( key, value ),null);
    }

    public ListRepresentation getAutoIndexedNodesByQuery( String query )
    {
        if ( query != null )
        {
            ReadableIndex<Node> index = graphDb.index().getNodeAutoIndexer().getAutoIndex();
            return toListNodeRepresentation(index.query( query ),null);
        }
        return toListNodeRepresentation();
    }

    public ListRepresentation getIndexedRelationships( String indexName,
            final String key, final String value )
    {
        if ( !graphDb.index().existsForRelationships( indexName ) )
            throw new NotFoundException();

        Index<Relationship> index = graphDb.index().forRelationships( indexName );

        final IndexRepresentation indexRepresentation = new RelationshipIndexRepresentation(indexName);

        IterableWrapper<Representation, Relationship> result =
                new IterableWrapper<Representation, Relationship>(index.get(key, value)) {
            @Override
            protected Representation underlyingObjectToObject(Relationship relationship) {
                return new IndexedEntityRepresentation(relationship,
                        key, value, indexRepresentation);
            }
        };
        return new ListRepresentation( RepresentationType.RELATIONSHIP, result);
    }

    public ListRepresentation getIndexedRelationshipsByQuery( String indexName,
            String query, String sort )
    {
        return getIndexedRelationshipsByQuery( indexName, null, query, sort );
    }

    public ListRepresentation getIndexedRelationshipsByQuery( String indexName,
            String key, String query, String sort )
    {
        if ( !graphDb.index().existsForRelationships( indexName ) )
            throw new NotFoundException();

        if ( query == null )
        {
            return toListRelationshipRepresentation();
        }
        Index<Relationship> index = graphDb.index().forRelationships( indexName );

        IndexResultOrder order = getOrdering( sort );
        QueryContext queryCtx = order.updateQueryContext( new QueryContext(
                query ) );

        return toListRelationshipRepresentation(index.query( key, queryCtx ), order);
    }

    public Representation getAutoIndexedRelationships( String key, String value )
    {
        ReadableRelationshipIndex index = graphDb.index().getRelationshipAutoIndexer().getAutoIndex();

        return toListRelationshipRepresentation(index.get( key, value ),null);
    }

    public ListRepresentation getAutoIndexedRelationshipsByQuery( String query )
    {
        ReadableRelationshipIndex index = graphDb.index().getRelationshipAutoIndexer().getAutoIndex();

        final IndexHits<Relationship> results = query != null ? index.query(query) : null;
        return toListRelationshipRepresentation(results,null);
    }

    // Traversal

    public ListRepresentation traverse( long startNode,
            Map<String, Object> description, final TraverserReturnType returnType )
    {
        Node node = graphDb.getNodeById( startNode );

        TraversalDescription traversalDescription = TraversalDescriptionBuilder.from( description );
        final Iterable<Path> paths = traversalDescription.traverse(node);
        return toListPathRepresentation(paths, returnType);
    }

    private ListRepresentation toListPathRepresentation(final Iterable<Path> paths, final TraverserReturnType returnType) {
        final IterableWrapper<Representation, Path> result = new IterableWrapper<Representation, Path>(paths)
        {
            @Override
            protected Representation underlyingObjectToObject(Path position)
            {
                return returnType.toRepresentation(position);
            }
        };
        return new ListRepresentation( returnType.repType, result );
    }

    public ListRepresentation pagedTraverse( String traverserId,
            TraverserReturnType returnType )
    {
        Lease lease = leases.getLeaseById( traverserId );
        if ( lease == null )
        {
            throw new NotFoundException( String.format(
                    "The traverser with id [%s] was not found", traverserId ) );
        }

        PagedTraverser traverser = lease.getLeasedItemAndRenewLease();
        List<Path> paths = traverser.next();

        if ( paths != null )
        {
            return toListPathRepresentation(paths,returnType);
        }
        else
        {
            leases.remove( traverserId );
            // Yuck.
            throw new NotFoundException(
                    String.format(
                            "The results for paged traverser with id [%s] have been fully enumerated",
                            traverserId ) );
        }
    }

    public String createPagedTraverser( long nodeId,
            Map<String, Object> description, int pageSize, int leaseTime )
    {
        Node node = graphDb.getNodeById( nodeId );

        TraversalDescription traversalDescription = TraversalDescriptionBuilder.from( description );

        PagedTraverser traverser = new PagedTraverser(
                traversalDescription.traverse( node ), pageSize );

        return leases.createLease( leaseTime, traverser ).getId();
    }

    public boolean removePagedTraverse( String traverserId )
    {
        Lease lease = leases.getLeaseById( traverserId );
        if ( lease == null )
        {
            return false;
        }
        else
        {
            leases.remove( lease.getId() );
            return true;
        }
    }

    // Graph algos

    @SuppressWarnings( "rawtypes" )
    public PathRepresentation findSinglePath( long startId, long endId,
            Map<String, Object> map )
    {
        FindParams findParams = new FindParams( startId, endId, map ).invoke();
        PathFinder finder = findParams.getFinder();
        Node startNode = findParams.getStartNode();
        Node endNode = findParams.getEndNode();

        Path path = finder.findSinglePath( startNode, endNode );
        if ( path == null )
        {
            throw new NotFoundException();
        }
        return findParams.pathRepresentationOf( path );
    }

    @SuppressWarnings( { "rawtypes", "unchecked" } )
    public ListRepresentation findPaths( long startId, long endId,
            Map<String, Object> map )
    {
        final FindParams findParams = new FindParams( startId, endId, map ).invoke();
        PathFinder finder = findParams.getFinder();
        Node startNode = findParams.getStartNode();
        Node endNode = findParams.getEndNode();

        Iterable paths = finder.findAllPaths( startNode, endNode );

        IterableWrapper<PathRepresentation, Path> pathRepresentations = new IterableWrapper<PathRepresentation, Path>(
                paths )
        {
            @Override
            protected PathRepresentation underlyingObjectToObject( Path path )
            {
                return findParams.pathRepresentationOf( path );
            }
        };

        return new ListRepresentation( RepresentationType.PATH,
                pathRepresentations );
    }

    private class FindParams
    {
        private final long startId;
        private final long endId;
        private final Map<String, Object> map;
        private Node startNode;
        private Node endNode;
        private PathFinder<? extends Path> finder;
        @SuppressWarnings( "rawtypes" )
        private PathRepresentationCreator representationCreator = PATH_REPRESENTATION_CREATOR;

        public FindParams( final long startId, final long endId,
                final Map<String, Object> map )
        {
            this.startId = startId;
            this.endId = endId;
            this.map = map;
        }

        public Node getStartNode()
        {
            return startNode;
        }

        public Node getEndNode()
        {
            return endNode;
        }

        public PathFinder<? extends Path> getFinder()
        {
            return finder;
        }

        @SuppressWarnings( "unchecked" )
        public PathRepresentation<? extends Path> pathRepresentationOf(
                Path path )
        {
            return representationCreator.from( path );
        }

        public FindParams invoke()
        {
            startNode = graphDb.getNodeById( startId );
            endNode = graphDb.getNodeById( endId );

            Integer maxDepthObj = (Integer) map.get( "max_depth" );
            int maxDepth = ( maxDepthObj != null ) ? maxDepthObj : 1;

            RelationshipExpander expander = RelationshipExpanderBuilder.describeRelationships( map );

            String algorithm = (String) map.get( "algorithm" );
            algorithm = ( algorithm != null ) ? algorithm : "shortestPath";

            finder = getAlgorithm( algorithm, expander, maxDepth );
            return this;
        }

        private PathFinder<? extends Path> getAlgorithm( String algorithm,
                RelationshipExpander expander, int maxDepth )
        {
            if ( algorithm.equals( "shortestPath" ) )
            {
                return GraphAlgoFactory.shortestPath( expander, maxDepth );
            }
            else if ( algorithm.equals( "allSimplePaths" ) )
            {
                return GraphAlgoFactory.allSimplePaths( expander, maxDepth );
            }
            else if ( algorithm.equals( "allPaths" ) )
            {
                return GraphAlgoFactory.allPaths( expander, maxDepth );
            }
            else if ( algorithm.equals( "dijkstra" ) )
            {
                String costProperty = (String) map.get( "cost_property" );
                Number defaultCost = (Number) map.get( "default_cost" );
                CostEvaluator<Double> costEvaluator = defaultCost == null ? CommonEvaluators.doubleCostEvaluator( costProperty )
                        : CommonEvaluators.doubleCostEvaluator( costProperty,
                                defaultCost.doubleValue() );
                representationCreator = WEIGHTED_PATH_REPRESENTATION_CREATOR;
                return GraphAlgoFactory.dijkstra( expander, costEvaluator );
            }

            throw new RuntimeException( "Failed to find matching algorithm" );
        }
    }

    /*
     * This enum binds the parameter-string-to-result-order mapping and
     * the kind of results returned. This is not correct in general but
     * at the time of writing it is the way things are done and is
     * quite handy. Feel free to rip out if requirements change.
     */
    private enum IndexResultOrder
    {
        INDEX_ORDER
        {
            @Override
            QueryContext updateQueryContext( QueryContext original )
            {
                return original.sort( Sort.INDEXORDER );
            }
        }
        ,RELEVANCE_ORDER
        {
            @Override
            QueryContext updateQueryContext( QueryContext original )
            {
                return original.sort( Sort.RELEVANCE );
            }
        },
        SCORE_ORDER
        {
            @Override
            QueryContext updateQueryContext( QueryContext original )
            {
                return original.sortByScore();
            }
        },
        NONE
        {
            @Override
            Representation getRepresentationFor( Representation delegate,
                    float score )
            {
                return delegate;
            }

            @Override
            QueryContext updateQueryContext( QueryContext original )
            {
                return original;
            }
        };

        Representation getRepresentationFor( Representation delegate,
                float score )
        {
            if ( delegate instanceof NodeRepresentation )
            {
                return new ScoredNodeRepresentation(
                        (NodeRepresentation) delegate, score );
            }
            if ( delegate instanceof RelationshipRepresentation )
            {
                return new ScoredRelationshipRepresentation(
                        (RelationshipRepresentation) delegate, score );
            }
            return delegate;
        }

        abstract QueryContext updateQueryContext( QueryContext original );
    }

    private final IndexResultOrder getOrdering( String order )
    {
        if ( INDEX_ORDER.equalsIgnoreCase( order ) )
        {
            return IndexResultOrder.INDEX_ORDER;
        }
        else if ( RELEVANCE_ORDER.equalsIgnoreCase( order ) )
        {
            return IndexResultOrder.RELEVANCE_ORDER;
        }
        else if ( SCORE_ORDER.equalsIgnoreCase( order ) )
        {
            return IndexResultOrder.SCORE_ORDER;
        }
        else
        {
            return IndexResultOrder.NONE;
        }
    }

    private interface PathRepresentationCreator<T extends Path>
    {
        PathRepresentation<T> from( T path );
    }

    private static final PathRepresentationCreator<Path> PATH_REPRESENTATION_CREATOR = new PathRepresentationCreator<Path>()
    {
        @Override
        public PathRepresentation<Path> from( Path path )
        {
            return new PathRepresentation<Path>( path );
        }
    };

    private static final PathRepresentationCreator<WeightedPath> WEIGHTED_PATH_REPRESENTATION_CREATOR = new PathRepresentationCreator<WeightedPath>()
    {
        @Override
        public PathRepresentation<WeightedPath> from( WeightedPath path )
        {
            return new WeightedPathRepresentation( path );
        }
    };
    
    private void assertIsLegalIndexName(String indexName)
    {
        if(indexName == null || indexName.equals("")) 
        {
            throw new IllegalArgumentException("Index name must not be empty.");
        }
    }
}
