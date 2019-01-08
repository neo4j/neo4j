/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.unsafe.batchinsert.internal;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.schema.ConstraintCreator;
import org.neo4j.graphdb.schema.IndexCreator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

public class FileSystemClosingBatchInserter implements BatchInserter, IndexConfigStoreProvider
{
    private final BatchInserter delegate;
    private final IndexConfigStoreProvider configStoreProvider;
    private final FileSystemAbstraction fileSystem;

    public FileSystemClosingBatchInserter( BatchInserter delegate, IndexConfigStoreProvider configStoreProvider,
            FileSystemAbstraction fileSystem )
    {
        this.delegate = delegate;
        this.configStoreProvider = configStoreProvider;
        this.fileSystem = fileSystem;
    }

    @Override
    public long createNode( Map<String,Object> properties, Label... labels )
    {
        return delegate.createNode( properties, labels );
    }

    @Override
    public void createNode( long id, Map<String,Object> properties, Label... labels )
    {
        delegate.createNode( id, properties, labels );
    }

    @Override
    public boolean nodeExists( long nodeId )
    {
        return delegate.nodeExists( nodeId );
    }

    @Override
    public void setNodeProperties( long node, Map<String,Object> properties )
    {
        delegate.setNodeProperties( node, properties );
    }

    @Override
    public boolean nodeHasProperty( long node, String propertyName )
    {
        return delegate.nodeHasProperty( node, propertyName );
    }

    @Override
    public void setNodeLabels( long node, Label... labels )
    {
        delegate.setNodeLabels( node, labels );
    }

    @Override
    public Iterable<Label> getNodeLabels( long node )
    {
        return delegate.getNodeLabels( node );
    }

    @Override
    public boolean nodeHasLabel( long node, Label label )
    {
        return delegate.nodeHasLabel( node, label );
    }

    @Override
    public boolean relationshipHasProperty( long relationship, String propertyName )
    {
        return delegate.relationshipHasProperty( relationship, propertyName );
    }

    @Override
    public void setNodeProperty( long node, String propertyName, Object propertyValue )
    {
        delegate.setNodeProperty( node, propertyName, propertyValue );
    }

    @Override
    public void setRelationshipProperty( long relationship, String propertyName, Object propertyValue )
    {
        delegate.setRelationshipProperty( relationship, propertyName, propertyValue );
    }

    @Override
    public Map<String,Object> getNodeProperties( long nodeId )
    {
        return delegate.getNodeProperties( nodeId );
    }

    @Override
    public Iterable<Long> getRelationshipIds( long nodeId )
    {
        return delegate.getRelationshipIds( nodeId );
    }

    @Override
    public Iterable<BatchRelationship> getRelationships( long nodeId )
    {
        return delegate.getRelationships( nodeId );
    }

    @Override
    public long createRelationship( long node1, long node2, RelationshipType type, Map<String,Object> properties )
    {
        return delegate.createRelationship( node1, node2, type, properties );
    }

    @Override
    public BatchRelationship getRelationshipById( long relId )
    {
        return delegate.getRelationshipById( relId );
    }

    @Override
    public void setRelationshipProperties( long rel, Map<String,Object> properties )
    {
        delegate.setRelationshipProperties( rel, properties );
    }

    @Override
    public Map<String,Object> getRelationshipProperties( long relId )
    {
        return delegate.getRelationshipProperties( relId );
    }

    @Override
    public void removeNodeProperty( long node, String property )
    {
        delegate.removeNodeProperty( node, property );
    }

    @Override
    public void removeRelationshipProperty( long relationship, String property )
    {
        delegate.removeRelationshipProperty( relationship, property );
    }

    @Override
    public IndexCreator createDeferredSchemaIndex( Label label )
    {
        return delegate.createDeferredSchemaIndex( label );
    }

    @Override
    public ConstraintCreator createDeferredConstraint( Label label )
    {
        return delegate.createDeferredConstraint( label );
    }

    @Override
    public void shutdown()
    {
        delegate.shutdown();
        closeFileSystem();
    }

    @Override
    public String getStoreDir()
    {
        return delegate.getStoreDir();
    }

    public FileSystemAbstraction getFileSystem()
    {
        return fileSystem;
    }

    private void closeFileSystem()
    {
        try
        {
            fileSystem.close();
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    @Override
    public IndexConfigStore getIndexStore()
    {
        return configStoreProvider.getIndexStore();
    }
}
