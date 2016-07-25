/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.HashSet;
import java.util.Set;

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.unsafe.impl.batchimport.Configuration;
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
 *
 * The format stores entities in batches, each batch having a small header containing number of bytes
 * and number of entities.
 */
public class InputCache implements Closeable
{
    public static final String MAIN = "main";

    private static final String HEADER = "-header";
    private static final String NODES = "nodes";
    private static final String RELATIONSHIPS = "relationships";
    private static final String NODES_HEADER = NODES + HEADER;
    private static final String RELATIONSHIPS_HEADER = RELATIONSHIPS + HEADER;

    static final byte SAME_GROUP = 0;
    static final byte NEW_GROUP = 1;
    static final byte PROPERTY_KEY_TOKEN = 0;
    static final byte LABEL_TOKEN = 1;
    static final byte RELATIONSHIP_TYPE_TOKEN = 2;
    static final byte GROUP_TOKEN = 3;
    static final byte HIGH_TOKEN_TYPE = 4;
    static final short HAS_FIRST_PROPERTY_ID = -1;
    static final byte HAS_LABEL_FIELD = 3;
    static final byte LABEL_REMOVAL = 1;
    static final byte LABEL_ADDITION = 2;
    static final byte END_OF_LABEL_CHANGES = 0;
    static final byte HAS_TYPE_ID = 2;
    static final byte SAME_TYPE = 0;
    static final byte NEW_TYPE = 1;
    static final byte END_OF_HEADER = -2;
    static final short END_OF_ENTITIES = -3;
    static final int NO_ENTITIES = 0;
    static final long END_OF_CACHE = 0L;

    private final FileSystemAbstraction fs;
    private final File cacheDirectory;
    private final RecordFormats recordFormats;
    private final Configuration config;

    private final int bufferSize;
    private final Set<String> subTypes = new HashSet<>();
    private final int batchSize;

    public InputCache( FileSystemAbstraction fs, File cacheDirectory, RecordFormats recordFormats,
            Configuration config )
    {
        this( fs, cacheDirectory, recordFormats, config, (int) ByteUnit.kibiBytes( 512 ), 10_000 );
    }

    /**
     * @param fs {@link FileSystemAbstraction} to use
     * @param cacheDirectory directory for placing the cached files
     * @param config import configuration
     * @param bufferSize buffer size for writing/reading cache files
     * @param batchSize number of entities in each batch
     */
    public InputCache( FileSystemAbstraction fs, File cacheDirectory, RecordFormats recordFormats,
            Configuration config, int bufferSize, int batchSize )
    {
        this.fs = fs;
        this.cacheDirectory = cacheDirectory;
        this.recordFormats = recordFormats;
        this.config = config;
        this.bufferSize = bufferSize;
        this.batchSize = batchSize;
    }

    public Receiver<InputNode[],IOException> cacheNodes( String subType ) throws IOException
    {
        return new InputNodeCacher( channel( NODES, subType, "rw" ), channel( NODES_HEADER, subType, "rw" ),
                recordFormats, bufferSize, batchSize );
    }

    public Receiver<InputRelationship[],IOException> cacheRelationships( String subType ) throws
            IOException
    {
        return new InputRelationshipCacher( channel( RELATIONSHIPS, subType, "rw" ),
                channel( RELATIONSHIPS_HEADER, subType, "rw" ), recordFormats, bufferSize, batchSize );
    }

    private StoreChannel channel( String type, String subType, String mode ) throws IOException
    {
        return fs.open( file( type, subType ), mode );
    }

    private File file( String type, String subType )
    {
        subTypes.add( subType );
        return new File( cacheDirectory, "input-" + type + "-" + subType );
    }

    public InputIterable<InputNode> nodes( String subType, boolean deleteAfterUse )
    {
        return entities( new ThrowingSupplier<InputIterator<InputNode>, IOException>()
        {
            @Override
            public InputIterator<InputNode> get() throws IOException
            {
                return new InputNodeReader( channel( NODES, subType, "r" ), channel( NODES_HEADER, subType, "r" ),
                        bufferSize, deleteAction( deleteAfterUse, NODES, NODES_HEADER, subType ),
                        config.maxNumberOfProcessors() );
            }
        } );
    }

    public InputIterable<InputRelationship> relationships( String subType, boolean deleteAfterUse )
    {
        return entities( new ThrowingSupplier<InputIterator<InputRelationship>, IOException>()
        {
            @Override
            public InputIterator<InputRelationship> get() throws IOException
            {
                return new InputRelationshipReader( channel( RELATIONSHIPS, subType, "r" ),
                        channel( RELATIONSHIPS_HEADER, subType, "r" ), bufferSize,
                        deleteAction( deleteAfterUse, RELATIONSHIPS, RELATIONSHIPS_HEADER, subType ),
                        config.maxNumberOfProcessors() );
            }
        } );
    }

    protected Runnable deleteAction( boolean deleteAfterUse, String type, String header, String subType )
    {
        if ( !deleteAfterUse )
        {
            return () -> {};
        }

        return () ->
        {
            fs.deleteFile( file( type, subType ) );
            fs.deleteFile( file( header, subType ) );
            subTypes.remove( subType );
        };
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
        for ( String subType : subTypes )
        {
            fs.deleteFile( file( NODES, subType ) );
            fs.deleteFile( file( RELATIONSHIPS, subType ) );
            fs.deleteFile( file( NODES_HEADER, subType ) );
            fs.deleteFile( file( RELATIONSHIPS_HEADER, subType ) );
        }
    }
}
