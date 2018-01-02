/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.input;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.unsafe.impl.batchimport.InputIterable;
import org.neo4j.unsafe.impl.batchimport.InputIterator;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;

/**
 * Cache of streams of {@link InputNode} or {@link InputRelationship} from an {@link Input} instance.
 * Useful since {@link ParallelBatchImporter} may require multiple passes over the input data and so
 * consecutive passes will be served by this thing instead.
 *
 * <pre>
 * Properties format:
 * - 2B property count, or {@link #HAS_FIRST_PROPERTY_ID} or {@link #END_OF_ENTITIES}
 * - property...:
 *   - 4B token id (token string->id mapping is in header file)
 *   - ?B value, see {@link ValueType}
 * </pre>
 *
 * <pre>
 * Group format:
 * - 1B, {@link #SAME_GROUP} or {@link #NEW_GROUP}
 *   IF {@link #NEW_GROUP}
 *   - 4B group if (if {@link #NEW_GROUP}
 *   - 4B token id
 * </pre>
 *
 * <pre>
 * Node format:
 * - properties (see "Properties format")
 * - group (see "Group format")
 * - id
 *   - ?B id value, see {@link ValueType}
 * - labels
 *   - 1B label mode, {@link #HAS_LABEL_FIELD} or {@link #LABEL_ADDITION} or {@link #LABEL_REMOVAL} or
 *     {@link #END_OF_LABEL_CHANGES}
 *     WHILE NOT {@link #END_OF_LABEL_CHANGES}
 *     - 4B token id, to add or remove
 *     - 1B label mode, next mode
 * </pre>
 *
 * <pre>
 * Relationship format:
 * - properties (see "Properties format")
 * - specific id:
 *   - 1B specific id boolean, {@link #SPECIFIC_ID} or {@link #UNSPECIFIED_ID}
 *     IF {@link #SPECIFIC_ID}
 *     - 8B specific relationship id
 * - start node group (see "Group format")
 * - end node group (see "Group format")
 * - start node id
 *   - ?B id value, see {@link ValueType}
 * - end node id
 *   - ?B id value, see {@link ValueType}
 * - type
 *   - 1B mode, {@link #SAME_TYPE} or {@link #NEW_TYPE} or {@link #HAS_TYPE_ID}
 *     IF {@link #HAS_TYPE_ID}
 *     4B type id
 *     ELSE IF {@link #NEW_TYPE}
 *     4B token id
 * </pre>
 */
public class InputCache implements Closeable
{
    private static final String HEADER = "-header";
    private static final String NODES = "nodes";
    private static final String RELATIONSHIPS = "relationships";
    private static final String NODES_HEADER = NODES + HEADER;
    private static final String RELATIONSHIPS_HEADER = RELATIONSHIPS + HEADER;

    static final byte SAME_GROUP = 0;
    static final byte NEW_GROUP = 1;
    static final byte TOKEN = 1;
    static final short HAS_FIRST_PROPERTY_ID = -1;
    static final byte HAS_LABEL_FIELD = 3;
    static final byte LABEL_REMOVAL = 1;
    static final byte LABEL_ADDITION = 2;
    static final byte END_OF_LABEL_CHANGES = 0;
    static final byte SPECIFIC_ID = 1;
    static final byte UNSPECIFIED_ID = 0;
    static final byte HAS_TYPE_ID = 2;
    static final byte SAME_TYPE = 0;
    static final byte NEW_TYPE = 1;
    static final byte END_OF_HEADER = 0;
    static final short END_OF_ENTITIES = -2;

    private final FileSystemAbstraction fs;
    private final File cacheDirectory;
    private final int bufferSize;

    public InputCache( FileSystemAbstraction fs, File cacheDirectory )
    {
        this( fs, cacheDirectory, (int) ByteUnit.kibiBytes( 512 ) );
    }

    public InputCache( FileSystemAbstraction fs, File cacheDirectory, int bufferSize )
    {
        this.fs = fs;
        this.cacheDirectory = cacheDirectory;
        this.bufferSize = bufferSize;
    }

    public Receiver<InputNode[],IOException> cacheNodes() throws IOException
    {
        return new InputNodeCacher( channel( NODES, "rw" ), channel( NODES_HEADER, "rw" ), bufferSize );
    }

    public Receiver<InputRelationship[],IOException> cacheRelationships() throws IOException
    {
        return new InputRelationshipCacher( channel( RELATIONSHIPS, "rw" ),
                channel( RELATIONSHIPS_HEADER, "rw" ), bufferSize );
    }

    private StoreChannel channel( String type, String mode ) throws IOException
    {
        return fs.open( file( type ), mode );
    }

    private File file( String type )
    {
        return new File( cacheDirectory, "input-" + type );
    }

    public InputIterable<InputNode> nodes()
    {
        return entities( new ThrowingSupplier<InputIterator<InputNode>, IOException>()
        {
            @Override
            public InputIterator<InputNode> get() throws IOException
            {
                return new InputNodeReader( channel( NODES, "r" ), channel( NODES_HEADER, "r" ), bufferSize );
            }
        } );
    }

    public InputIterable<InputRelationship> relationships()
    {
        return entities( new ThrowingSupplier<InputIterator<InputRelationship>, IOException>()
        {
            @Override
            public InputIterator<InputRelationship> get() throws IOException
            {
                return new InputRelationshipReader( channel( RELATIONSHIPS, "r" ),
                        channel( RELATIONSHIPS_HEADER, "r" ), bufferSize );
            }
        } );
    }

    private <T extends InputEntity> InputIterable<T> entities(
            final ThrowingSupplier<InputIterator<T>, IOException> factory )
    {
        return new InputIterable<T>()
        {
            @Override
            public InputIterator<T> iterator()
            {
                try
                {
                    return factory.get();
                }
                catch ( IOException e )
                {
                    throw new InputException( "Unable to read cached relationship", e );
                }
            }

            @Override
            public boolean supportsMultiplePasses()
            {
                return true;
            }
        };
    }

    @Override
    public void close() throws IOException
    {
        fs.deleteFile( file( NODES ) );
        fs.deleteFile( file( RELATIONSHIPS ) );
        fs.deleteFile( file( NODES_HEADER ) );
        fs.deleteFile( file( RELATIONSHIPS_HEADER ) );
    }
}
