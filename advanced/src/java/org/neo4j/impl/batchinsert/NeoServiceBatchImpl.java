package org.neo4j.impl.batchinsert;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.NotFoundException;
import org.neo4j.api.core.NotInTransactionException;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.ReturnableEvaluator;
import org.neo4j.api.core.StopEvaluator;
import org.neo4j.api.core.Transaction;
import org.neo4j.api.core.Traverser;
import org.neo4j.api.core.Traverser.Order;
import org.neo4j.impl.cache.LruCache;
import org.neo4j.impl.nioneo.store.InvalidRecordException;

class NeoServiceBatchImpl implements NeoService
{
    final BatchInserterImpl batchInserter;
    
    private final LruCache<Long,NodeBatchImpl> nodes = 
        new LruCache<Long,NodeBatchImpl>( "NodeCache", 10000, null )
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
        new LruCache<Long,RelationshipBatchImpl>( "RelCache", 100000, null )
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
    
    NeoServiceBatchImpl( BatchInserterImpl batchInserter )
    {
        this.batchInserter = batchInserter;
    }
    
    BatchInserterImpl getBatchInserter()
    {
        return batchInserter;
    }

    public Transaction beginTx()
    {
        return new FakeTransaction();
    }

    public Node createNode()
    {
        long id = batchInserter.createNode( null );
        NodeBatchImpl node = new NodeBatchImpl( id, this, emptyProps() );
        nodes.put( id, node );
        return node;
    }
    
    static Map<String,Object> emptyProps()
    {
        return new HashMap<String,Object>();
    }

    public boolean enableRemoteShell()
    {
        return false;
    }

    public boolean enableRemoteShell( Map<String,Serializable> initialProperties )
    {
        return false;
    }

    public Iterable<Node> getAllNodes()
    {
        throw new UnsupportedOperationException( "Batch inserter mode" );
    }

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

    public Node getReferenceNode()
    {
        return getNodeById( 0 );
    }

    public Relationship getRelationshipById( long id )
    {
        RelationshipBatchImpl rel = rels.get( id );
        if ( rel == null )
        {
            try
            {
                SimpleRelationship simpleRel = 
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

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        throw new UnsupportedOperationException( "Batch inserter mode" );
    }

    public void shutdown()
    {
        nodes.clear();
        rels.clear();
    }

    static class FakeTransaction implements Transaction
    {
        public void failure()
        {
            throw new NotInTransactionException( "Batch insert mode, " + 
                "failure is not an option." );
        }

        public void finish()
        {
        }

        public void success()
        {
        }
    }
    
    private static class NodeBatchImpl implements Node
    {
        private final NeoServiceBatchImpl neoService;
        
        private final long id;
        private final Map<String,Object> properties;
        
        NodeBatchImpl( long id, NeoServiceBatchImpl neoService, 
            Map<String,Object> properties )
        {
            this.id = id;
            this.neoService = neoService;
            this.properties = properties;
        }
        
        public Relationship createRelationshipTo( Node otherNode, 
            RelationshipType type )
        {
            long relId = neoService.getBatchInserter().createRelationship( id, 
                otherNode.getId(), type, null );
            RelationshipBatchImpl rel = new RelationshipBatchImpl( 
                new SimpleRelationship( (int)relId, (int) id, 
                    (int) otherNode.getId(), type ), neoService, emptyProps() );
            neoService.addRelationshipToCache( id, rel );
            return rel;
        }
        
        Map<String,Object> getProperties()
        {
            return properties;
        }

        public void delete()
        {
            throw new UnsupportedOperationException();
        }

        public long getId()
        {
            return id;
        }

        private RelIterator newRelIterator( Direction dir, 
            RelationshipType[] types )
        {
            Iterable<Long> relIds = 
                neoService.getBatchInserter().getRelationshipIds( id );
            return new RelIterator( neoService, relIds, id, dir, types ); 
        }
        
        public Iterable<Relationship> getRelationships()
        {
            return newRelIterator( Direction.BOTH, null );
        }
        
        public Iterable<Relationship> getRelationships( 
            RelationshipType... types )
        {
            return newRelIterator( Direction.BOTH, types );
        }

        public Iterable<Relationship> getRelationships( Direction dir )
        {
            return newRelIterator( dir, null );
        }

        public Iterable<Relationship> getRelationships( RelationshipType type, 
            Direction dir )
        {
            return newRelIterator( dir, new RelationshipType[] { type } );
        }

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

        public boolean hasRelationship()
        {
            Iterator<Relationship> relItr = 
                newRelIterator( Direction.BOTH, null );
            return relItr.hasNext();
        }

        public boolean hasRelationship( RelationshipType... types )
        {
            Iterator<Relationship> relItr = 
                newRelIterator( Direction.BOTH, types );
            return relItr.hasNext();
        }

        public boolean hasRelationship( Direction dir )
        {
            Iterator<Relationship> relItr = 
                newRelIterator( dir, null );
            return relItr.hasNext();
        }

        public boolean hasRelationship( RelationshipType type, Direction dir )
        {
            Iterator<Relationship> relItr = 
                newRelIterator( dir, new RelationshipType[] { type } );
            return relItr.hasNext();
        }

        public Traverser traverse( Order traversalOrder, 
            StopEvaluator stopEvaluator, 
            ReturnableEvaluator returnableEvaluator, 
            RelationshipType relationshipType, Direction direction )
        {
            throw new UnsupportedOperationException( "Batch inserter mode" );
        }

        public Traverser traverse( Order traversalOrder, 
            StopEvaluator stopEvaluator, 
            ReturnableEvaluator returnableEvaluator, 
            RelationshipType firstRelationshipType, Direction firstDirection, 
            RelationshipType secondRelationshipType, Direction secondDirection )
        {
            throw new UnsupportedOperationException( "Batch inserter mode" );
        }

        public Traverser traverse( Order traversalOrder, 
            StopEvaluator stopEvaluator, 
            ReturnableEvaluator returnableEvaluator, 
            Object... relationshipTypesAndDirections )
        {
            throw new UnsupportedOperationException( "Batch inserter mode" );
        }

        public Object getProperty( String key )
        {
            Object val = properties.get( key );
            if ( val == null )
            {
                throw new NotFoundException( key );
            }
            return val;
        }

        public Object getProperty( String key, Object defaultValue )
        {
            Object val = properties.get( key );
            if ( val == null )
            {
                return defaultValue;
            }
            return val;
        }

        public Iterable<String> getPropertyKeys()
        {
            return properties.keySet();
        }

        public Iterable<Object> getPropertyValues()
        {
            return properties.values();
        }

        public boolean hasProperty( String key )
        {
            return properties.containsKey( key );
        }

        public Object removeProperty( String key )
        {
            Object val = properties.remove( key );
            if ( val == null )
            {
                throw new NotFoundException( "Property " + key );
            }
            return val;
        }

        public void setProperty( String key, Object value )
        {
            properties.put( key, value );
        }

        public boolean equals( Object o )
        {
            if ( !(o instanceof Node) )
            {
                return false;
            }
            return this.getId() == ((Node) o).getId();

        }

        public int hashCode()
        {
            return (int) id;
        }
    }
    
    private static class RelationshipBatchImpl implements Relationship
    {
        private final SimpleRelationship rel;
        private final NeoServiceBatchImpl neoService;
        private final Map<String,Object> properties;
        
        
        RelationshipBatchImpl( SimpleRelationship rel, 
            NeoServiceBatchImpl neoService, Map<String,Object> properties )
        {
            this.rel = rel;
            this.neoService = neoService;
            this.properties = properties;
        }
        
        Map<String,Object> getProperties()
        {
            return properties;
        }
        
        public void delete()
        {
            throw new UnsupportedOperationException( "Batch inserter mode" );
        }

        public Node getEndNode()
        {
            return neoService.getNodeById( rel.getEndNode() );
        }

        public long getId()
        {
            return rel.getId();
        }

        public Node[] getNodes()
        {
            return new Node[] { getStartNode(), getEndNode() };
        }

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

        public Node getStartNode()
        {
            return neoService.getNodeById( rel.getStartNode() );
        }

        public RelationshipType getType()
        {
            return rel.getType();
        }

        public boolean isType( RelationshipType type )
        {
            return rel.getType().equals( type );
        }

        public Object getProperty( String key )
        {
            Object val = properties.get( key );
            if ( val == null )
            {
                throw new NotFoundException( key );
            }
            return val;
        }

        public Object getProperty( String key, Object defaultValue )
        {
            Object val = properties.get( key );
            if ( val == null )
            {
                return defaultValue;
            }
            return val;
        }

        public Iterable<String> getPropertyKeys()
        {
            return properties.keySet();
        }

        public Iterable<Object> getPropertyValues()
        {
            return properties.values();
        }

        public boolean hasProperty( String key )
        {
            return properties.containsKey( key );
        }

        public Object removeProperty( String key )
        {
            Object val = properties.remove( key );
            if ( val == null )
            {
                throw new NotFoundException( "Property " + key );
            }
            return val;
        }

        public void setProperty( String key, Object value )
        {
            properties.put( key, value );
        }

        public boolean equals( Object o )
        {
            if ( !(o instanceof Relationship) )
            {
                return false;
            }
            return this.getId() == ((Relationship) o).getId();

        }

        public int hashCode()
        {
            return (int) rel.getId();
        }
    }
    
    void addRelationshipToCache( long id, RelationshipBatchImpl rel )
    {
        rels.put( id, rel );
    }
    
    static class RelIterator implements 
        Iterable<Relationship>, Iterator<Relationship>
    {

        private final NeoServiceBatchImpl neoService;
        private final Iterable<Long> relIds;
        private final Iterator<Long> relItr;
        private final long nodeId;
        private final Direction dir;
        private final RelationshipType[] types;
        
        private Relationship nextElement;
        
        RelIterator( NeoServiceBatchImpl neoService, Iterable<Long> relIds, 
            long nodeId, Direction dir, RelationshipType[] types )
        {
            this.neoService = neoService;
            this.relIds = relIds;
            this.relItr = relIds.iterator();
            this.nodeId = nodeId;
            this.dir = dir;
            this.types = types;
        }
        
        public Iterator<Relationship> iterator()
        {
            return new RelIterator( neoService, relIds, nodeId, dir, types );
        }

        public boolean hasNext()
        {
            getNextElement();
            if ( nextElement != null )
            {
                return true;
            }
            return false;
        }

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
                    neoService.getRelationshipById( relItr.next() );
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
                nextElement = possibleRel;
            }
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }
}