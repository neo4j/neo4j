/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.unsafe.batchinsert;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.event.KernelEventHandler;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.schema.Schema;
import org.neo4j.kernel.PlaceboTransaction;
import org.neo4j.kernel.impl.cache.LruCache;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException;

class BatchGraphDatabaseImpl implements GraphDatabaseService
{
    final BatchInserter batchInserter;

    private final LruCache<Long,NodeBatchImpl> nodes =
        new LruCache<Long,NodeBatchImpl>( "NodeCache", 10000 )
        {
            @Override
            public void elementCleaned( NodeBatchImpl node )
            {
                Map<String,Object> properties = node.getProperties();
                if ( properties != null )
                {
                    batchInserter.setNodeProperties( node.getId(), properties );
                }
            }
        };

    private final LruCache<Long,RelationshipBatchImpl> rels =
        new LruCache<Long,RelationshipBatchImpl>( "RelCache", 10000 )
        {
            @Override
            public void elementCleaned( RelationshipBatchImpl rel )
            {
                Map<String,Object> properties = rel.getProperties();
                if ( properties != null )
                {
                    batchInserter.setRelationshipProperties( rel.getId(),
                        properties );
                }
            }
        };

    BatchGraphDatabaseImpl( String storeDir )
    {
        this.batchInserter = new BatchInserterImpl( storeDir );
    }

    BatchGraphDatabaseImpl( String storeDir, Map<String, String> stringParams )
    {
        this.batchInserter = new BatchInserterImpl( storeDir, stringParams );
    }

    BatchGraphDatabaseImpl( String storeDir, FileSystemAbstraction fileSystem,
            Map<String, String> stringParams )
    {
        this.batchInserter = new BatchInserterImpl( storeDir, fileSystem, stringParams );
    }
    
    /**
     * Intended for use in tests only.
     */
    BatchGraphDatabaseImpl( BatchInserter batchInserter )
    {
        this.batchInserter = batchInserter;
    }

    BatchInserter getBatchInserter()
    {
        return batchInserter;
    }

    @Override
    public Transaction beginTx()
    {
        return new FakeTransaction();
    }
    
    @Override
    public Node createNode()
    {
        long id = batchInserter.createNode( null );
        NodeBatchImpl node = new NodeBatchImpl( id, this, emptyProps() );
        nodes.put( id, node );
        return node;
    }

    @Override
    public Node createNode( Label... labels )
    {
        throw unsupportedOperation();
    }

    private static UnsupportedOperationException unsupportedOperation()
    {
        return new UnsupportedOperationException( "Batch inserter mode" );
    }

    static Map<String,Object> emptyProps()
    {
        return new HashMap<String,Object>();
    }

    @Override
    public Iterable<Node> getAllNodes()
    {
        throw unsupportedOperation();
    }

    public Iterable<Relationship> getAllRelationships()
    {
        throw unsupportedOperation();
    }
    
    @Override
    public Node getNodeById( long id )
    {
        NodeBatchImpl node = nodes.get( id );
        if ( node == null )
        {
            try
            {
                node = new NodeBatchImpl( id, this,
                    batchInserter.getNodeProperties( id ) );
                nodes.put( id, node );
            }
            catch ( InvalidRecordException e )
            {
                throw new NotFoundException( e );
            }
        }
        return node;
    }

    @Override
    public Node getReferenceNode()
    {
        return getNodeById( 0 );
    }

    @Override
    public Relationship getRelationshipById( long id )
    {
        RelationshipBatchImpl rel = rels.get( id );
        if ( rel == null )
        {
            try
            {
                BatchRelationship simpleRel =
                    batchInserter.getRelationshipById( id );
                Map<String,Object> props =
                    batchInserter.getRelationshipProperties( id );
                rel = new RelationshipBatchImpl( simpleRel, this, props );
                rels.put( id, rel );
            }
            catch ( InvalidRecordException e )
            {
                throw new NotFoundException( e );
            }
        }
        return rel;
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes()
    {
        throw unsupportedOperation();
    }

    @Override
    public void shutdown()
    {
        clearCaches();
        batchInserter.shutdown();
    }

    static class FakeTransaction implements Transaction
    {
        @Override
        public void failure()
        {
            throw new NotInTransactionException( "Batch insert mode, " +
                "failure is not an option." );
        }

        @Override
        public void finish()
        {
        }

        @Override
        public void success()
        {
        }
        
        @Override
        public Lock acquireWriteLock( PropertyContainer entity )
        {
            return PlaceboTransaction.NO_LOCK;
        }
        
        @Override
        public Lock acquireReadLock( PropertyContainer entity )
        {
            return PlaceboTransaction.NO_LOCK;
        }
    }

    private static class NodeBatchImpl implements Node
    {
        private final BatchGraphDatabaseImpl graphDbService;

        private final long id;
        private final Map<String,Object> properties;

        NodeBatchImpl( long id, BatchGraphDatabaseImpl graphDbService,
            Map<String,Object> properties )
        {
            this.id = id;
            this.graphDbService = graphDbService;
            this.properties = properties;
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            return graphDbService;
        }

        @Override
        public Relationship createRelationshipTo( Node otherNode,
            RelationshipType type )
        {
            long relId = graphDbService.getBatchInserter().createRelationship( id,
                otherNode.getId(), type, null );
            RelationshipBatchImpl rel = new RelationshipBatchImpl(
                    new BatchRelationship( relId, id, otherNode.getId(), type ),
                    graphDbService, emptyProps() );
            graphDbService.addRelationshipToCache( relId, rel );
            return rel;
        }

        Map<String,Object> getProperties()
        {
            return properties;
        }

        @Override
        public void delete()
        {
            throw unsupportedOperation();
        }

        @Override
        public long getId()
        {
            return id;
        }

        private RelIterator newRelIterator( Direction dir,
            RelationshipType[] types )
        {
            Iterable<Long> relIds =
                graphDbService.getBatchInserter().getRelationshipIds( id );
            return new RelIterator( graphDbService, relIds, id, dir, types );
        }

        @Override
        public Iterable<Relationship> getRelationships()
        {
            return newRelIterator( Direction.BOTH, null );
        }

        @Override
        public Iterable<Relationship> getRelationships(
            RelationshipType... types )
        {
            return newRelIterator( Direction.BOTH, types );
        }

        @Override
        public Iterable<Relationship> getRelationships( Direction direction,
                RelationshipType... types )
        {
            return newRelIterator( direction, types );
        }
        
        @Override
        public Iterable<Relationship> getRelationships( Direction dir )
        {
            return newRelIterator( dir, null );
        }

        @Override
        public Iterable<Relationship> getRelationships( RelationshipType type,
            Direction dir )
        {
            return newRelIterator( dir, new RelationshipType[] { type } );
        }

        @Override
        public Relationship getSingleRelationship( RelationshipType type,
            Direction dir )
        {
            Iterator<Relationship> relItr =
                newRelIterator( dir, new RelationshipType[] { type } );
            if ( relItr.hasNext() )
            {
                Relationship rel = relItr.next();
                if ( relItr.hasNext() )
                {
                    throw new NotFoundException( "More than one relationship[" +
                        type + ", " + dir + "] found for " + this );
                }
                return rel;
            }
            return null;
        }

        @Override
        public boolean hasRelationship()
        {
            return newRelIterator( Direction.BOTH, null ).hasNext();
        }

        @Override
        public boolean hasRelationship( RelationshipType... types )
        {
            return newRelIterator( Direction.BOTH, types ).hasNext();
        }

        @Override
        public boolean hasRelationship( Direction direction, RelationshipType... types )
        {
            return newRelIterator( direction, types ).hasNext();
        }
        
        @Override
        public boolean hasRelationship( Direction dir )
        {
            Iterator<Relationship> relItr =
                newRelIterator( dir, null );
            return relItr.hasNext();
        }

        @Override
        public boolean hasRelationship( RelationshipType type, Direction dir )
        {
            return newRelIterator( dir, new RelationshipType[] { type } ).hasNext();
        }

        /* Tentative expansion API
        public Expansion<Relationship> expandAll()
        {
            return Traversal.expanderForAllTypes().expand( this );
        }

        public Expansion<Relationship> expand( RelationshipType type )
        {
            return expand( type, Direction.BOTH );
        }

        public Expansion<Relationship> expand( RelationshipType type,
                Direction direction )
        {
            return Traversal.expanderForTypes( type, direction ).expand(
                    this );
        }

        public Expansion<Relationship> expand( Direction direction )
        {
            return Traversal.expanderForAllTypes( direction ).expand(
                    this );
        }

        public Expansion<Relationship> expand( RelationshipExpander expander )
        {
            return Traversal.expander( expander ).expand( this );
        }
        */

        @Override
        public Traverser traverse( Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            RelationshipType relationshipType, Direction direction )
        {
            throw unsupportedOperation();
        }

        @Override
        public Traverser traverse( Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            RelationshipType firstRelationshipType, Direction firstDirection,
            RelationshipType secondRelationshipType, Direction secondDirection )
        {
            throw unsupportedOperation();
        }

        @Override
        public Traverser traverse( Order traversalOrder,
            StopEvaluator stopEvaluator,
            ReturnableEvaluator returnableEvaluator,
            Object... relationshipTypesAndDirections )
        {
            throw unsupportedOperation();
        }

        @Override
        public void addLabel( Label label )
        {
            throw unsupportedOperation();
        }

        @Override
        public void removeLabel( Label label )
        {
            throw unsupportedOperation();
        }

        @Override
        public boolean hasLabel( Label label )
        {
            throw unsupportedOperation();
        }
        
        @Override
        public Iterable<Label> getLabels()
        {
            throw unsupportedOperation();
        }

        @Override
        public Object getProperty( String key )
        {
            Object val = properties.get( key );
            if ( val == null )
            {
                throw new NotFoundException( key );
            }
            return val;
        }

        @Override
        public Object getProperty( String key, Object defaultValue )
        {
            Object val = properties.get( key );
            if ( val == null )
            {
                return defaultValue;
            }
            return val;
        }

        @Override
        public Iterable<String> getPropertyKeys()
        {
            return properties.keySet();
        }

        @Override
        public Iterable<Object> getPropertyValues()
        {
            return properties.values();
        }

        @Override
        public boolean hasProperty( String key )
        {
            return properties.containsKey( key );
        }

        @Override
        public Object removeProperty( String key )
        {
            Object val = properties.remove( key );
            if ( val == null )
            {
                throw new NotFoundException( "Property " + key );
            }
            return val;
        }

        @Override
        public void setProperty( String key, Object value )
        {
            properties.put( key, value );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof Node) )
            {
                return false;
            }
            return this.getId() == ((Node) o).getId();

        }

        @Override
        public int hashCode()
        {
            return (int) ( id ^ ( id >>> 32 ) );
        }
    }

    private static class RelationshipBatchImpl implements Relationship
    {
        private final BatchRelationship rel;
        private final BatchGraphDatabaseImpl graphDbService;
        private final Map<String,Object> properties;

        RelationshipBatchImpl( BatchRelationship rel,
            BatchGraphDatabaseImpl graphDbService, Map<String,Object> properties )
        {
            this.rel = rel;
            this.graphDbService = graphDbService;
            this.properties = properties;
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            return graphDbService;
        }

        Map<String,Object> getProperties()
        {
            return properties;
        }

        @Override
        public void delete()
        {
            throw unsupportedOperation();
        }

        @Override
        public Node getEndNode()
        {
            return graphDbService.getNodeById( rel.getEndNode() );
        }

        @Override
        public long getId()
        {
            return rel.getId();
        }

        @Override
        public Node[] getNodes()
        {
            return new Node[] { getStartNode(), getEndNode() };
        }

        @Override
        public Node getOtherNode( Node node )
        {
            Node startNode = getStartNode();
            Node endNode = getEndNode();
            if ( node.equals( endNode ) )
            {
                return startNode;
            }
            if ( node.equals( startNode ) )
            {
                return endNode;
            }
            throw new IllegalArgumentException( "" + node );
        }

        @Override
        public Node getStartNode()
        {
            return graphDbService.getNodeById( rel.getStartNode() );
        }

        @Override
        public RelationshipType getType()
        {
            return rel.getType();
        }

        @Override
        public boolean isType( RelationshipType type )
        {
            return rel.getType().equals( type );
        }

        @Override
        public Object getProperty( String key )
        {
            Object val = properties.get( key );
            if ( val == null )
            {
                throw new NotFoundException( key );
            }
            return val;
        }

        @Override
        public Object getProperty( String key, Object defaultValue )
        {
            Object val = properties.get( key );
            if ( val == null )
            {
                return defaultValue;
            }
            return val;
        }

        @Override
        public Iterable<String> getPropertyKeys()
        {
            return properties.keySet();
        }

        @Override
        public Iterable<Object> getPropertyValues()
        {
            return properties.values();
        }

        @Override
        public boolean hasProperty( String key )
        {
            return properties.containsKey( key );
        }

        @Override
        public Object removeProperty( String key )
        {
            Object val = properties.remove( key );
            if ( val == null )
            {
                throw new NotFoundException( "Property " + key );
            }
            return val;
        }

        @Override
        public void setProperty( String key, Object value )
        {
            properties.put( key, value );
        }

        @Override
        public boolean equals( Object o )
        {
            if ( !(o instanceof Relationship) )
            {
                return false;
            }
            return this.getId() == ((Relationship) o).getId();

        }

        @Override
        public int hashCode()
        {
            return (int) ( rel.getId() ^ ( rel.getId() >>> 32 ) );
        }
    }

    void addRelationshipToCache( long id, RelationshipBatchImpl rel )
    {
        rels.put( id, rel );
    }

    static class RelIterator implements
        Iterable<Relationship>, Iterator<Relationship>
    {

        private final BatchGraphDatabaseImpl graphDbService;
        private final Iterable<Long> relIds;
        private final Iterator<Long> relItr;
        private final long nodeId;
        private final Direction dir;
        private final RelationshipType[] types;

        private Relationship nextElement;

        RelIterator( BatchGraphDatabaseImpl graphDbService, Iterable<Long> relIds,
            long nodeId, Direction dir, RelationshipType[] types )
        {
            this.graphDbService = graphDbService;
            this.relIds = relIds;
            this.relItr = relIds.iterator();
            this.nodeId = nodeId;
            this.dir = dir;
            this.types = types;
        }

        @Override
        public Iterator<Relationship> iterator()
        {
            return new RelIterator( graphDbService, relIds, nodeId, dir, types );
        }

        @Override
        public boolean hasNext()
        {
            getNextElement();
            if ( nextElement != null )
            {
                return true;
            }
            return false;
        }

        @Override
        public Relationship next()
        {
            getNextElement();
            if ( nextElement != null )
            {
                Relationship returnVal = nextElement;
                nextElement = null;
                return returnVal;
            }
            throw new NoSuchElementException();
        }

        private void getNextElement()
        {
            while ( nextElement == null && relItr.hasNext() )
            {
                Relationship possibleRel =
                    graphDbService.getRelationshipById( relItr.next() );
                if ( dir == Direction.OUTGOING &&
                    possibleRel.getEndNode().getId() == nodeId )
                {
                    continue;
                }
                if ( dir == Direction.INCOMING &&
                    possibleRel.getStartNode().getId() == nodeId )
                {
                    continue;
                }
                if ( types != null )
                {
                    for ( RelationshipType type : types )
                    {
                        if ( type.name().equals(
                            possibleRel.getType().name() ) )
                        {
                            nextElement = possibleRel;
                            break;
                        }
                    }
                }
                else
                {
                    nextElement = possibleRel;
                }
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    void clearCaches()
    {
        nodes.clear();
        rels.clear();
    }

    @Override
    public KernelEventHandler registerKernelEventHandler(
            KernelEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> TransactionEventHandler<T> registerTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public KernelEventHandler unregisterKernelEventHandler(
            KernelEventHandler handler )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Schema schema()
    {
        throw new UnsupportedOperationException("Schema modification is currently not available through the Batch API.");
    }

    @Override
    public <T> TransactionEventHandler<T> unregisterTransactionEventHandler(
            TransactionEventHandler<T> handler )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexManager index()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ResourceIterable<Node> findNodesByLabelAndProperty( Label myLabel, String propertyName, Object value )
    {
        throw new UnsupportedOperationException();
    }
}