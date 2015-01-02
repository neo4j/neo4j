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
package org.neo4j.index.impl.lucene;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

/**
 * The {@link BatchInserter} version of {@link LuceneIndexImplementation}. Indexes
 * created and populated using {@link BatchInserterIndex}s from this provider
 * are compatible with {@link Index}s from {@link LuceneIndexImplementation}.
 */
public class LuceneBatchInserterIndexProviderNewImpl implements BatchInserterIndexProvider
{
    private final BatchInserter inserter;
    private final Map<IndexIdentifier, LuceneBatchInserterIndex> indexes =
            new HashMap<IndexIdentifier, LuceneBatchInserterIndex>();
    final IndexStore indexStore;
    final EntityType nodeEntityType;
    final EntityType relationshipEntityType;

    public LuceneBatchInserterIndexProviderNewImpl( final BatchInserter inserter )
    {
        this.inserter = inserter;
        this.indexStore = ((BatchInserterImpl) inserter).getIndexStore();
        this.nodeEntityType = new EntityType()
        {
            @Override
            public Document newDocument( Object entityId )
            {
                return IndexType.newBaseDocument( (Long) entityId );
            }
            
            @Override
            public Class<? extends PropertyContainer> getType()
            {
                return Node.class;
            }
        };
        this.relationshipEntityType = new EntityType()
        {
            @Override
            public Document newDocument( Object entityId )
            {
                RelationshipId relId = null;
                if ( entityId instanceof Long )
                {
                    BatchRelationship relationship = inserter
                            .getRelationshipById( (Long) entityId );
                    relId = new RelationshipId( relationship.getId(), relationship.getStartNode(),
                            relationship.getEndNode() );
                }
                else if ( entityId instanceof RelationshipId )
                {
                    relId = (RelationshipId) entityId;
                }
                else
                {
                    throw new IllegalArgumentException( "Ids of type " + entityId.getClass()
                            + " are not supported." );
                }
                Document doc = IndexType.newBaseDocument( relId.id );
                doc.add( new Field( LuceneIndex.KEY_START_NODE_ID, "" + relId.startNode,
                        Store.YES, org.apache.lucene.document.Field.Index.NOT_ANALYZED ) );
                doc.add( new Field( LuceneIndex.KEY_END_NODE_ID, "" + relId.endNode,
                        Store.YES, org.apache.lucene.document.Field.Index.NOT_ANALYZED ) );
                return doc;
            }
            
            @Override
            public Class<? extends PropertyContainer> getType()
            {
                return Relationship.class;
            }
        };
    }
    
    @Override
    public BatchInserterIndex nodeIndex( String indexName, Map<String, String> config )
    {
        config( Node.class, indexName, config );
        return index( new IndexIdentifier( LuceneCommand.NODE, nodeEntityType, indexName ), config );
    }

    private Map<String, String> config( Class<? extends PropertyContainer> cls,
            String indexName, Map<String, String> config )
    {
        // TODO Doesn't look right
        if ( config != null )
        {
            config = MapUtil.stringMap( new HashMap<String, String>( config ),
                    IndexManager.PROVIDER, LuceneIndexImplementation.SERVICE_NAME );
            indexStore.setIfNecessary( cls, indexName, config );
            return config;
        }
        else
        {
            return indexStore.get( cls, indexName );
        }
    }

    @Override
    public BatchInserterIndex relationshipIndex( String indexName, Map<String, String> config )
    {
        config( Relationship.class, indexName, config );
        return index( new IndexIdentifier( LuceneCommand.RELATIONSHIP, relationshipEntityType, indexName ), config );
    }

    private BatchInserterIndex index( IndexIdentifier identifier, Map<String, String> config )
    {
        // We don't care about threads here... c'mon... it's a
        // single-threaded batch inserter
        LuceneBatchInserterIndex index = indexes.get( identifier );
        if ( index == null )
        {
            index = new LuceneBatchInserterIndex( new File(inserter.getStoreDir()),
                    identifier,
                    config );
            indexes.put( identifier, index );
        }
        return index;
    }
    
    @Override
    public void shutdown()
    {
        for ( LuceneBatchInserterIndex index : indexes.values() )
        {
            index.shutdown();
        }
    }
}
