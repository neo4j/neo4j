/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.batchinsert.internal;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.io.fs.FileSystemAbstraction;

public class FileSystemClosingBatchInserter implements BatchInserter
{
    private final BatchInserter delegate;
    private final FileSystemAbstraction fileSystem;

    public FileSystemClosingBatchInserter( BatchInserter delegate, FileSystemAbstraction fileSystem )
    {
        this.delegate = delegate;
        this.fileSystem = fileSystem;
    }

    @Override
    public long createNode()
    {
        return delegate.createNode();
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
    public long createRelationship( long node1, long node2, RelationshipType type )
    {
        return delegate.createRelationship( node1, node2, type );
    }

    @Override
    public void shutdown()
    {
        delegate.shutdown();
        closeFileSystem();
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
}
