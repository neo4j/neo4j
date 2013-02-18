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
package org.neo4j.kernel.impl.batchinsert;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.impl.index.IndexStore;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.unsafe.batchinsert.BatchInserters;

/**
 * @deprecated see {@link BatchInserters} for what to use.
 */
@Deprecated
public class BatchInserterImpl implements BatchInserter
{
    private final org.neo4j.unsafe.batchinsert.BatchInserterImpl batchInserter;

    public BatchInserterImpl( String storeDir )
    {
        batchInserter = (org.neo4j.unsafe.batchinsert.BatchInserterImpl) BatchInserters.inserter( storeDir );
    }

    public BatchInserterImpl( String storeDir, FileSystemAbstraction fileSystem,
            Map<String,String> stringParams )
    {
        batchInserter = (org.neo4j.unsafe.batchinsert.BatchInserterImpl) BatchInserters.inserter(
                storeDir, fileSystem, stringParams );
    }
    
    public BatchInserterImpl( String storeDir,
        Map<String,String> stringParams )
    {
        batchInserter = (org.neo4j.unsafe.batchinsert.BatchInserterImpl) BatchInserters.inserter(
                storeDir, stringParams );
    }

    @Override
    public boolean nodeHasProperty( long node, String propertyName )
    {
        return batchInserter.nodeHasProperty( node, propertyName );
    }

    @Override
    public boolean relationshipHasProperty( long relationship,
            String propertyName )
    {
        return batchInserter.relationshipHasProperty( relationship,
                propertyName );
    }

    @Override
    public void setNodeProperty( long node, String propertyName,
            Object propertyValue )
    {
        batchInserter.setNodeProperty( node, propertyName, propertyValue );
    }

    @Override
    public void setRelationshipProperty( long relationship,
            String propertyName, Object propertyValue )
    {
        batchInserter.setRelationshipProperty( relationship, propertyName,
                propertyValue );
    }

    @Override
    public void removeNodeProperty( long node, String propertyName )
    {
        batchInserter.removeNodeProperty( node, propertyName );
    }

    @Override
    public void removeRelationshipProperty( long relationship,
            String propertyName )
    {
        batchInserter.removeRelationshipProperty( relationship, propertyName );
    }

    @Override
    public long createNode( Map<String,Object> properties )
    {
        return batchInserter.createNode( properties );
    }

    @Override
    public void createNode( long id, Map<String,Object> properties )
    {
        batchInserter.createNode( id, properties );
    }

    @Override
    public long createRelationship( long node1, long node2, RelationshipType
        type, Map<String,Object> properties )
    {
        return batchInserter.createRelationship( node1, node2, type, properties );
    }

    @Override
    public void setNodeProperties( long node, Map<String,Object> properties )
    {
        batchInserter.setNodeProperties( node, properties );
    }

    @Override
    public void setRelationshipProperties( long rel,
        Map<String,Object> properties )
    {
        batchInserter.setRelationshipProperties( rel, properties );
    }

    @Override
    public boolean nodeExists( long nodeId )
    {
        return batchInserter.nodeExists( nodeId );
    }

    @Override
    public Map<String,Object> getNodeProperties( long nodeId )
    {
        return batchInserter.getNodeProperties( nodeId );
    }

    @Override
    public Iterable<Long> getRelationshipIds( long nodeId )
    {
        return batchInserter.getRelationshipIds( nodeId );
    }

    @Override
    public Iterable<SimpleRelationship> getRelationships( long nodeId )
    {
        return batchInserter.getSimpleRelationships( nodeId );
    }

    @Override
    public SimpleRelationship getRelationshipById( long relId )
    {
        return batchInserter.getSimpleRelationshipById( relId );
    }

    @Override
    public Map<String,Object> getRelationshipProperties( long relId )
    {
        return batchInserter.getRelationshipProperties( relId );
    }

    @Override
    public void shutdown()
    {
        batchInserter.shutdown();
    }

    @Override
    public String toString()
    {
        return "BatchInserterImpl(old)[" + batchInserter.getStoreDir() + "]";
    }

    @Override
    public String getStore()
    {
        return batchInserter.getStoreDir();
    }

    @Override
    public long getReferenceNode()
    {
        return batchInserter.getReferenceNode();
    }

    /**
     * @deprecated as of Neo4j 1.7
     */
    @Deprecated
    @Override
    public GraphDatabaseService getGraphDbService()
    {
        return batchInserter.getBatchGraphDbService();
    }

    public IndexStore getIndexStore()
    {
        return batchInserter.getIndexStore();
    }

    public IdGeneratorFactory getIdGeneratorFactory()
    {
        return batchInserter.getIdGeneratorFactory();
    }
}

