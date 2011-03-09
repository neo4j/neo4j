/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.web;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.collection.IterableWrapper;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.Traversal;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.DatabaseBlockedException;
import org.neo4j.server.rest.domain.EndNodeNotFoundException;
import org.neo4j.server.rest.domain.RelationshipExpanderBuilder;
import org.neo4j.server.rest.domain.StartNodeNotFoundException;
import org.neo4j.server.rest.domain.StartNodeSameAsEndNodeException;
import org.neo4j.server.rest.domain.TraversalDescriptionBuilder;
import org.neo4j.server.rest.domain.TraverserReturnType;
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
import org.neo4j.server.rest.repr.WeightedPathRepresentation;

// TODO: move this to another package. domain?
public class DatabaseActions
{
    private final AbstractGraphDatabase graphDb;

    public DatabaseActions( Database database )
    {
        this.graphDb = database.graph;
    }

    private Node node( long id ) throws NodeNotFoundException
    {
        try
        {
            return graphDb.getNodeById( id );
        } catch ( NotFoundException e )
        {
            throw new NodeNotFoundException();
        }
    }

    private Relationship relationship( long id ) throws RelationshipNotFoundException
    {
        try
        {
            return graphDb.getRelationshipById( id );
        } catch ( NotFoundException e )
        {
            throw new RelationshipNotFoundException();
        }
    }

    private <T extends PropertyContainer> T set( T entity, Map<String, Object> properties )
            throws PropertyValueException
    {
        if ( properties != null )
        {
            for ( Map.Entry<String, Object> property : properties.entrySet() )
            {
                try
                {
                    entity.setProperty( property.getKey(), property( property.getValue() ) );
                } catch ( IllegalArgumentException ex )
                {
                    throw new PropertyValueException( property.getKey(), property.getValue() );
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
                    array = (Object[]) Array.newInstance( object.getClass(), collection.size() );
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
        Transaction tx = graphDb.beginTx();
        try
        {
            result = new NodeRepresentation( set( graphDb.createNode(), properties ) );
            tx.success();
        } finally
        {
            tx.finish();
        }
        return result;
    }

    public NodeRepresentation getNode( long nodeId ) throws NodeNotFoundException
    {
        return new NodeRepresentation( node( nodeId ) );
    }

    public void deleteNode( long nodeId ) throws NodeNotFoundException, OperationFailureException
    {
        Node node = node( nodeId );
        Transaction tx = graphDb.beginTx();
        try
        {
            node.delete();
            tx.success();
        } finally
        {
            try
            {
                tx.finish();
            } catch ( TransactionFailureException e )
            {
                throw new OperationFailureException();
            }
        }
    }

    public NodeRepresentation getReferenceNode()
    {
        return new NodeRepresentation( graphDb.getReferenceNode() );
    }

    // Node properties

    public Representation getNodeProperty( long nodeId, String key ) throws NodeNotFoundException,
            NoSuchPropertyException
    {
        Node node = node( nodeId );
        try
        {
            return PropertiesRepresentation.value( node.getProperty( key ) );
        } catch ( NotFoundException e )
        {
            throw new NoSuchPropertyException( node, key );
        }
    }

    public void setNodeProperty( long nodeId, String key, Object value )
            throws PropertyValueException, NodeNotFoundException
    {
        Node node = node( nodeId );
        value = property( value );
        Transaction tx = graphDb.beginTx();
        try
        {
            node.setProperty( key, value );
            tx.success();
        } catch ( IllegalArgumentException e )
        {
            throw new PropertyValueException( key, value );
        } finally
        {
            tx.finish();
        }
    }

    public void removeNodeProperty( long nodeId, String key ) throws NodeNotFoundException,
            NoSuchPropertyException
    {
        Node node = node( nodeId );
        Transaction tx = graphDb.beginTx();
        try
        {
            if ( node.removeProperty( key ) == null )
            {
                throw new NoSuchPropertyException( node, key );
            }
            tx.success();
        } finally
        {
            tx.finish();
        }
    }

    public PropertiesRepresentation getAllNodeProperties( long nodeId )
            throws NodeNotFoundException
    {
        return new PropertiesRepresentation( node( nodeId ) );
    }

    public void setAllNodeProperties( long nodeId, Map<String, Object> properties )
            throws PropertyValueException, NodeNotFoundException
    {
        Node node = node( nodeId );
        Transaction tx = graphDb.beginTx();
        try
        {
            set( clear( node ), properties );
            tx.success();
        } finally
        {
            tx.finish();
        }
    }

    public void removeAllNodeProperties( long nodeId ) throws NodeNotFoundException
    {
        Node node = node( nodeId );
        Transaction tx = graphDb.beginTx();
        try
        {
            clear( node );
            tx.success();
        } finally
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

    public IndexRepresentation createNodeIndex( Map<String, Object> indexSpecification )
    {
        final String indexName = (String) indexSpecification.get( "name" );
        if ( indexSpecification.containsKey( "config" ) )
        {

            @SuppressWarnings( "unchecked" )
            Map<String, String> config = (Map<String, String>) indexSpecification.get( "config" );
            graphDb.index().forNodes( indexName, config );

            return new NodeIndexRepresentation( indexName, config );
        }

        graphDb.index().forNodes( indexName );
        return new NodeIndexRepresentation( indexName, Collections.<String, String>emptyMap() );
    }


    public IndexRepresentation createRelationshipIndex( Map<String, Object> indexSpecification )
    {
        final String indexName = (String) indexSpecification.get( "name" );
        if ( indexSpecification.containsKey( "config" ) )
        {

            @SuppressWarnings( "unchecked" )
            Map<String, String> config = (Map<String, String>) indexSpecification.get( "config" );
            graphDb.index().forRelationships( indexName, config );

            return new RelationshipIndexRepresentation( indexName, config );
        }

        graphDb.index().forRelationships( indexName );
        return new RelationshipIndexRepresentation( indexName, Collections.<String, String>emptyMap() );
    }

    public boolean nodeIsIndexed( String indexName, String key, Object value, long nodeId ) throws DatabaseBlockedException
    {

        Index<Node> index = graphDb.index().forNodes( indexName );
        Transaction tx = graphDb.beginTx();
        try
        {
            Node expectedNode = graphDb.getNodeById( nodeId );
            IndexHits<Node> hits = index.get( key, value );
            boolean contains = iterableContains( hits, expectedNode );
            tx.success();
            return contains;
        } finally
        {
            tx.finish();
        }
    }

    public boolean relationshipIsIndexed( String indexName, String key, Object value, long relationshipId ) throws DatabaseBlockedException
    {

        Index<Relationship> index = graphDb.index().forRelationships( indexName );
        Transaction tx = graphDb.beginTx();
        try
        {
            Relationship expectedNode = graphDb.getRelationshipById( relationshipId );
            IndexHits<Relationship> hits = index.get( key, value );
            boolean contains = iterableContains( hits, expectedNode );
            tx.success();
            return contains;
        } finally
        {
            tx.finish();
        }
    }

    private <T> boolean iterableContains( Iterable<T> iterable, T expectedElement )
    {
        for ( T possibleMatch : iterable )
        {
            if ( possibleMatch.equals( expectedElement ) )
                return true;
        }
        return false;
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

    public RelationshipRepresentation createRelationship( long startNodeId, long endNodeId,
                                                          String type,
                                                          Map<String, Object> properties ) throws StartNodeNotFoundException,
            EndNodeNotFoundException, StartNodeSameAsEndNodeException, PropertyValueException
    {
        if ( startNodeId == endNodeId )
        {
            throw new StartNodeSameAsEndNodeException();
        }
        Node start, end;
        try
        {
            start = node( startNodeId );
        } catch ( NodeNotFoundException e )
        {
            throw new StartNodeNotFoundException();
        }
        try
        {
            end = node( endNodeId );
        } catch ( NodeNotFoundException e )
        {
            throw new EndNodeNotFoundException();
        }
        final RelationshipRepresentation result;
        Transaction tx = graphDb.beginTx();
        try
        {
            result = new RelationshipRepresentation( set( start.createRelationshipTo( end,
                    DynamicRelationshipType.withName( type ) ), properties ) );
            tx.success();
        } finally
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

    public void deleteRelationship( long relationshipId ) throws RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        Transaction tx = graphDb.beginTx();
        try
        {
            relationship.delete();
            tx.success();
        } finally
        {
            tx.finish();
        }
    }

    public ListRepresentation getNodeRelationships( long nodeId, RelationshipDirection direction,
                                                    Collection<String> types ) throws NodeNotFoundException
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
                expander = expander.add( DynamicRelationshipType.withName( type ),
                        direction.internal );
            }
        }
        return RelationshipRepresentation.list( expander.expand( node ) );
    }

    // Relationship properties

    public PropertiesRepresentation getAllRelationshipProperties( long relationshipId )
            throws RelationshipNotFoundException
    {
        return new PropertiesRepresentation( relationship( relationshipId ) );
    }

    public Representation getRelationshipProperty( long relationshipId, String key )
            throws NoSuchPropertyException, RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        try
        {
            return PropertiesRepresentation.value( relationship.getProperty( key ) );
        } catch ( NotFoundException e )
        {
            throw new NoSuchPropertyException( relationship, key );
        }
    }

    public void setAllRelationshipProperties( long relationshipId, Map<String, Object> properties )
            throws PropertyValueException, RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        Transaction tx = graphDb.beginTx();
        try
        {
            set( clear( relationship ), properties );
            tx.success();
        } finally
        {
            tx.finish();
        }
    }

    public void setRelationshipProperty( long relationshipId, String key, Object value )
            throws PropertyValueException, RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        value = property( value );
        Transaction tx = graphDb.beginTx();
        try
        {
            relationship.setProperty( key, value );
            tx.success();
        } catch ( IllegalArgumentException e )
        {
            throw new PropertyValueException( key, value );
        } finally
        {
            tx.finish();
        }
    }

    public void removeAllRelationshipProperties( long relationshipId )
            throws RelationshipNotFoundException
    {
        Relationship relationship = relationship( relationshipId );
        Transaction tx = graphDb.beginTx();
        try
        {
            clear( relationship );
            tx.success();
        } finally
        {
            tx.finish();
        }
    }

    public void removeRelationshipProperty( long relationshipId, String key )
            throws RelationshipNotFoundException, NoSuchPropertyException
    {
        Relationship relationship = relationship( relationshipId );
        Transaction tx = graphDb.beginTx();
        try
        {
            if ( relationship.removeProperty( key ) == null )
            {
                throw new NoSuchPropertyException( relationship, key );
            }
            tx.success();
        } finally
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

        @SuppressWarnings("boxing")
        String path( String indexName, String key, String value, long id )
        {
            return String.format( "%s/%s/%s/%s/%s", pathPrefix, indexName, key, value, id );
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

    public IndexedEntityRepresentation addToRelationshipIndex( String indexName, String key,
                                                               String value, long relationshipId )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Relationship relationship = graphDb.getRelationshipById( relationshipId );
            Index<Relationship> index = graphDb.index().forRelationships( indexName );
            index.add( relationship, key, value );
            tx.success();
            return new IndexedEntityRepresentation( relationship, key, value, new RelationshipIndexRepresentation( indexName, Collections.<String, String>emptyMap() ) );
        } finally
        {
            tx.finish();
        }
    }

    public IndexedEntityRepresentation addToNodeIndex( String indexName, String key,
                                                       String value, long nodeId )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Node node = graphDb.getNodeById( nodeId );
            Index<Node> index = graphDb.index().forNodes( indexName );
            index.add( node, key, value );
            tx.success();
            return new IndexedEntityRepresentation( node, key, value, new NodeIndexRepresentation( indexName, Collections.<String, String>emptyMap() ) );
        } finally
        {
            tx.finish();
        }
    }

    public void removeFromNodeIndex( String indexName, String key, String value, long id )
    {
        Index<Node> index = graphDb.index().forNodes( indexName );
        Transaction tx = graphDb.beginTx();
        try
        {
            index.remove( graphDb.getNodeById( id ), key, value );
            tx.success();
        } finally
        {
            tx.finish();
        }
    }

    public void removeFromNodeIndexNoValue( String indexName, String key, long id )
    {
        Index<Node> index = graphDb.index().forNodes( indexName );
        Transaction tx = graphDb.beginTx();
        try
        {
            index.remove( graphDb.getNodeById( id ), key );
            tx.success();
        } finally
        {
            tx.finish();
        }
    }
    
    public void removeFromNodeIndexNoKeyValue( String indexName, long id )
    {
        Index<Node> index = graphDb.index().forNodes( indexName );
        Transaction tx = graphDb.beginTx();
        try
        {
            index.remove( graphDb.getNodeById( id ) );
            tx.success();
        } finally
        {
            tx.finish();
        }
    }
    
    public void removeFromRelationshipIndex( String indexName, String key, String value, long id )
    {
        RelationshipIndex index = graphDb.index().forRelationships( indexName );
        Transaction tx = graphDb.beginTx();
        try
        {
            index.remove( graphDb.getRelationshipById( id ), key, value );
            tx.success();
        } finally
        {
            tx.finish();
        }
    }

    public void removeFromRelationshipIndexNoValue( String indexName, String key, long id )
    {
        RelationshipIndex index = graphDb.index().forRelationships( indexName );
        Transaction tx = graphDb.beginTx();
        try
        {
            index.remove( graphDb.getRelationshipById( id ), key );
            tx.success();
        } finally
        {
            tx.finish();
        }
    }
    
    public void removeFromRelationshipIndexNoKeyValue( String indexName, long id )
    {
        RelationshipIndex index = graphDb.index().forRelationships( indexName );
        Transaction tx = graphDb.beginTx();
        try
        {
            index.remove( graphDb.getRelationshipById( id ) );
            tx.success();
        } finally
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
        return new IndexedEntityRepresentation( node, key, value, new NodeIndexRepresentation( indexName, Collections.<String, String>emptyMap() ) );
    }

    public IndexedEntityRepresentation getIndexedRelationship( String indexName,
                                                               String key, String value, long id )
    {
        if ( !relationshipIsIndexed( indexName, key, value, id ) )
            throw new NotFoundException();
        Relationship node = graphDb.getRelationshipById( id );
        return new IndexedEntityRepresentation( node, key, value, new RelationshipIndexRepresentation( indexName, Collections.<String, String>emptyMap() ) );
    }


    public ListRepresentation getIndexedNodesByExactMatch( String indexName, String key,
                                               String value )
    {
        if ( !graphDb.index().existsForNodes( indexName ) )
            throw new NotFoundException();
        Index<Node> index = graphDb.index().forNodes( indexName );
        List<IndexedEntityRepresentation> representations = new ArrayList<IndexedEntityRepresentation>();

        Transaction tx = graphDb.beginTx();
        try
        {
            IndexRepresentation indexRepresentation = new NodeIndexRepresentation( indexName );
            for ( Node node : index.get( key, value ) )
            {
                representations.add( new IndexedEntityRepresentation( node, key, value, indexRepresentation ) );
            }
            tx.success();
            return new ListRepresentation( RepresentationType.NODE, representations );
        } finally
        {
            tx.finish();
        }
    }

    public ListRepresentation getIndexedNodesByQuery( String indexName, String key,
                                               String query )
    {
        if ( !graphDb.index().existsForNodes( indexName ) )
            throw new NotFoundException();
        Index<Node> index = graphDb.index().forNodes( indexName );
        List<Representation> representations = new ArrayList<Representation>();

        Transaction tx = graphDb.beginTx();
        try
        {
            for ( Node node : index.query( key, query ) )
            {
                representations.add( new NodeRepresentation( node ));
            }
            tx.success();
            return new ListRepresentation( RepresentationType.NODE, representations );
        } finally
        {
            tx.finish();
        }
    }


    public ListRepresentation getIndexedRelationships( String indexName, String key,
                                                       String value )
    {
        if ( !graphDb.index().existsForRelationships( indexName ) )
            throw new NotFoundException();
        List<IndexedEntityRepresentation> representations = new ArrayList<IndexedEntityRepresentation>();
        Index<Relationship> index = graphDb.index().forRelationships( indexName );

        Transaction tx = graphDb.beginTx();
        try
        {
            IndexRepresentation indexRepresentation = new RelationshipIndexRepresentation( indexName );
            for ( Relationship node : index.get( key, value ) )
            {
                representations.add( new IndexedEntityRepresentation( node, key, value, indexRepresentation ) );
            }
            tx.success();
            return new ListRepresentation( RepresentationType.RELATIONSHIP, representations );
        } finally
        {
            tx.finish();
        }
    }

    public ListRepresentation getIndexedRelationshipsByQuery( String indexName, String key,
                                                       String query )
    {
        if ( !graphDb.index().existsForRelationships( indexName ) )
            throw new NotFoundException();
        List<Representation> representations = new ArrayList<Representation>();
        Index<Relationship> index = graphDb.index().forRelationships( indexName );

        Transaction tx = graphDb.beginTx();
        try
        {
            for ( Relationship rel : index.query( key, query ) )
            {
                representations.add( new RelationshipRepresentation( rel ));
            }
            tx.success();
            return new ListRepresentation( RepresentationType.RELATIONSHIP, representations );
        } finally
        {
            tx.finish();
        }
    }

    // Traversal

    public ListRepresentation traverse( long startNode, Map<String, Object> description,
                                        TraverserReturnType returnType )
    {
        Node node = graphDb.getNodeById( startNode );

        List<Representation> result = new ArrayList<Representation>();

        TraversalDescription traversalDescription = TraversalDescriptionBuilder.from( description );
        for ( Path position : traversalDescription.traverse( node ) )
        {
            result.add( returnType.toRepresentation( position ) );
        }

        return new ListRepresentation( returnType.repType, result );
    }

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

        IterableWrapper<PathRepresentation, Path> pathRepresentations = new IterableWrapper<PathRepresentation, Path>( paths )
        {
            @Override
            protected PathRepresentation underlyingObjectToObject( Path path )
            {
                return findParams.pathRepresentationOf( path );
            }
        };

        return new ListRepresentation( RepresentationType.PATH, pathRepresentations );
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

        public FindParams( final long startId, final long endId, final Map<String, Object> map )
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
        public PathRepresentation<? extends Path> pathRepresentationOf( Path path )
        {
            return representationCreator.from( path );
        }

        public FindParams invoke()
        {
            startNode = graphDb.getNodeById( startId );
            endNode = graphDb.getNodeById( endId );

            Integer maxDepthObj = (Integer) map.get( "max depth" );
            int maxDepth = (maxDepthObj != null) ? maxDepthObj : 1;

            RelationshipExpander expander = RelationshipExpanderBuilder.describeRelationships( map );

            String algorithm = (String) map.get( "algorithm" );
            algorithm = (algorithm != null) ? algorithm : "shortestPath";

            finder = getAlgorithm( algorithm, expander, maxDepth );
            return this;
        }

        private PathFinder<? extends Path> getAlgorithm( String algorithm, RelationshipExpander expander, int maxDepth )
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
                String costProperty = (String) map.get( "cost property" );
                Number defaultCost = (Number) map.get( "default cost" );
                CostEvaluator<Double> costEvaluator = defaultCost == null ?
                        CommonEvaluators.doubleCostEvaluator( costProperty ) :
                        CommonEvaluators.doubleCostEvaluator( costProperty, defaultCost.doubleValue() );
                representationCreator = WEIGHTED_PATH_REPRESENTATION_CREATOR;
                return GraphAlgoFactory.dijkstra( expander, costEvaluator );
            }

            throw new RuntimeException( "Failed to find matching algorithm" );
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
    
    private static final PathRepresentationCreator<WeightedPath> WEIGHTED_PATH_REPRESENTATION_CREATOR =
            new PathRepresentationCreator<WeightedPath>()
    {
        @Override
        public PathRepresentation<WeightedPath> from( WeightedPath path )
        {
            return new WeightedPathRepresentation( path );
        }
    };
}
