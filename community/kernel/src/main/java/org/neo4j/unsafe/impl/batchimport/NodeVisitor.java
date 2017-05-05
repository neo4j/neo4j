package org.neo4j.unsafe.impl.batchimport;

import java.util.Arrays;

import org.neo4j.kernel.impl.store.InlineNodeLabels;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.id.IdRangeIterator;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Group;
import org.neo4j.unsafe.impl.batchimport.input.InputEntityVisitor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingLabelTokenRepository;
import org.neo4j.unsafe.impl.batchimport.store.BatchingTokenRepository.BatchingPropertyKeyTokenRepository;

public class NodeVisitor extends InputEntityVisitor.Adapter
{
    private final BatchingPropertyKeyTokenRepository propertyKeyTokenRepository;
    private final BatchingLabelTokenRepository labelTokenRepository;
    private final NodeStore nodeStore;
    private final NodeRecord nodeRecord;
    private PropertyBlock propertyBlocks[] = new PropertyBlock[100];
    private int propertyBlocksCursor;
    private final PropertyStore propertyStore;
    private final IdMapper idMapper;
    private IdRangeIterator nodeIds;
    private IdRangeIterator propertyIds;
    private final PropertyRecord propertyRecord;

    public NodeVisitor( NeoStores stores, BatchingPropertyKeyTokenRepository propertyKeyTokenRepository,
            BatchingLabelTokenRepository labelTokenRepository, IdMapper idMapper )
    {
        this.propertyKeyTokenRepository = propertyKeyTokenRepository;
        this.labelTokenRepository = labelTokenRepository;
        this.idMapper = idMapper;
        this.nodeStore = stores.getNodeStore();
        this.nodeRecord = nodeStore.newRecord();
        for ( int i = 0; i < propertyBlocks.length; i++ )
        {
            propertyBlocks[i] = new PropertyBlock();
        }
        this.propertyStore = stores.getPropertyStore();
        this.propertyRecord = propertyStore.newRecord();
    }

    @Override
    public boolean propertyId( long nextProp )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean property( String key, Object value )
    {
        encodeProperty( nextPropertyBlock(), key, value );
        return true;
    }

    private PropertyBlock nextPropertyBlock()
    {
        if ( propertyBlocksCursor == propertyBlocks.length )
        {
            propertyBlocks = Arrays.copyOf( propertyBlocks, propertyBlocksCursor * 2 );
            for ( int i = propertyBlocksCursor; i < propertyBlocks.length; i++ )
            {
                propertyBlocks[i] = new PropertyBlock();
            }
        }
        return propertyBlocks[propertyBlocksCursor++];
    }

    private void encodeProperty( PropertyBlock block, String key, Object value )
    {
        // TODO: dynamic record ids, batching of those
        propertyStore.encodeValue( block, propertyKeyTokenRepository.getOrCreateId( key ), value );
    }

    @Override
    public boolean id( long id, Group group )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean id( String id, Group group )
    {
        long nodeId = nextNodeId();
        nodeRecord.setId( nodeId );
        idMapper.put( id, nodeId, group );
        return true;
    }

    @Override
    public boolean labels( String[] labels )
    {
        long[] labelIds = labelTokenRepository.getOrCreateIds( labels );
        InlineNodeLabels.putSorted( nodeRecord, labelIds, null, nodeStore.getDynamicLabelStore() );
        return true;
    }

    @Override
    public boolean labelField( long labelField )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void endOfEntity()
    {
        // Write data to stores
        nodeRecord.setNextProp( createAndWritePropertyChain() );
        nodeRecord.setInUse( true );
        nodeStore.updateRecord( nodeRecord );

        // Clear internal state for next node
        propertyBlocksCursor = 0;
        nodeRecord.clear();
    }

    private long createAndWritePropertyChain()
    {
        if ( propertyBlocksCursor == 0 )
        {
            return Record.NO_NEXT_PROPERTY.longValue();
        }

        PropertyRecord currentRecord = propertyRecord( nextPropertyId() );
        long firstRecordId = currentRecord.getId();
        for ( int i = 0; i < propertyBlocksCursor; i++ )
        {
            PropertyBlock block = propertyBlocks[i];
            if ( currentRecord.size() + block.getSize() > PropertyType.getPayloadSize() )
            {
                // This record is full or couldn't fit this block, write it to property store
                long nextPropertyId = nextPropertyId();
                long prevId = currentRecord.getId();
                currentRecord.setNextProp( nextPropertyId );
                propertyStore.updateRecord( currentRecord );
                currentRecord = propertyRecord( nextPropertyId );
                currentRecord.setPrevProp( prevId );
            }

            // Add this block, there's room for it
            currentRecord.addPropertyBlock( block );
        }

        if ( currentRecord.size() > 0 )
        {
            propertyStore.updateRecord( currentRecord );
        }

        return firstRecordId;
    }

    private PropertyRecord propertyRecord( long nextPropertyId )
    {
        propertyRecord.clear();
        propertyRecord.setInUse( true );
        propertyRecord.setId( nextPropertyId );
        nodeRecord.setIdTo( propertyRecord );
        propertyRecord.setCreated();
        return propertyRecord;
    }

    private long nextNodeId()
    {
        long id;
        if ( nodeIds == null || (id = nodeIds.next()) == -1 )
        {
            nodeIds = new IdRangeIterator( nodeStore.nextIdBatch( 10_000 ) );
            id = nodeIds.next();
        }
        return id;
    }

    private long nextPropertyId()
    {
        long id;
        if ( propertyIds == null || (id = propertyIds.next()) == -1 )
        {
            propertyIds = new IdRangeIterator( propertyStore.nextIdBatch( 10_000 ) );
            id = propertyIds.next();
        }
        return id;
    }
}
