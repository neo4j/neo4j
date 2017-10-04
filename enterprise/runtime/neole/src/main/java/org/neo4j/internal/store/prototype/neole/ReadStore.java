/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.internal.store.prototype.neole;

import java.io.File;
import java.io.IOException;

import org.neo4j.internal.kernel.api.IndexPredicate;
import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.internal.store.cursors.MemoryManager;

import static org.neo4j.internal.store.prototype.neole.PartialPropertyCursor.NO_PROPERTIES;
import static org.neo4j.internal.store.prototype.neole.RelationshipCursor.NO_RELATIONSHIP;
import static org.neo4j.internal.store.prototype.neole.StoreFile.fixedSizeRecordFile;

public class ReadStore extends MemoryManager implements Read
{
    private static final String NODE_STORE = "neostore.nodestore.db", RELATIONSHIP_STORE =
            "neostore.relationshipstore.db",
            RELATIONSHIP_GROUP_STORE = "neostore.relationshipgroupstore.db", PROPERTY_STORE =
            "neostore.propertystore.db";
    private static final long INTEGER_MINUS_ONE = 0xFFFF_FFFFL;
    private final StoreFile nodes, relationships, relationshipGroups, properties;

    public ReadStore( File storeDir ) throws IOException
    {
        this.nodes = fixedSizeRecordFile( new File( storeDir, NODE_STORE ), NodeCursor.RECORD_SIZE );
        this.relationships =
                fixedSizeRecordFile( new File( storeDir, RELATIONSHIP_STORE ), RelationshipCursor.RECORD_SIZE );
        this.relationshipGroups =
                fixedSizeRecordFile( new File( storeDir, RELATIONSHIP_GROUP_STORE ), org.neo4j.internal.store
                        .prototype.neole.RelationshipGroupCursor.RECORD_SIZE );
        this.properties = fixedSizeRecordFile( new File( storeDir, PROPERTY_STORE ), PropertyCursor.RECORD_SIZE );
    }

    @Override
    public void nodeIndexSeek(
            IndexReference index,
            org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor,
            IndexPredicate... predicates )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeIndexScan( IndexReference index, org.neo4j.internal.kernel.api.NodeValueIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void nodeLabelScan( int label, org.neo4j.internal.kernel.api.NodeLabelIndexCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Scan<org.neo4j.internal.kernel.api.NodeLabelIndexCursor> nodeLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void allNodesScan( org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        ((NodeCursor) cursor).init( nodes, 0, nodes.maxReference );
    }

    @Override
    public Scan<org.neo4j.internal.kernel.api.NodeCursor> allNodesScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void singleNode( long reference, org.neo4j.internal.kernel.api.NodeCursor cursor )
    {
        ((NodeCursor) cursor).init( nodes, reference, reference );
    }

    @Override
    public void singleRelationship( long reference, RelationshipScanCursor cursor )
    {
        ((org.neo4j.internal.store.prototype.neole.RelationshipScanCursor) cursor)
                .init( relationships, reference, reference );
    }

    @Override
    public void allRelationshipsScan( RelationshipScanCursor cursor )
    {
        ((org.neo4j.internal.store.prototype.neole.RelationshipScanCursor) cursor).
                init( relationships, 0, relationships.maxReference );
    }

    @Override
    public Scan<RelationshipScanCursor> allRelationshipsScan()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipLabelScan( int label, RelationshipScanCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public Scan<RelationshipScanCursor> relationshipLabelScan( int label )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void relationshipGroups( long nodeReference, long reference, RelationshipGroupCursor cursor )
    {
        ((org.neo4j.internal.store.prototype.neole.RelationshipGroupCursor) cursor).
                init( relationshipGroups, relationships, nodeReference, reference );
    }

    @Override
    public void relationships( long nodeReference, long reference, RelationshipTraversalCursor cursor )
    {
        if ( reference == NO_RELATIONSHIP )
        {
            cursor.close();
        }
        else
        {
            ((org.neo4j.internal.store.prototype.neole.RelationshipTraversalCursor) cursor)
                    .init( relationships, nodeReference, reference );
        }
    }

    @Override
    public void nodeProperties( long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        if ( reference == NO_PROPERTIES )
        {
            cursor.close();
        }
        else
        {
            ((PropertyCursor) cursor).init( properties, reference );
        }
    }

    @Override
    public void relationshipProperties( long reference, org.neo4j.internal.kernel.api.PropertyCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void futureNodeReferenceRead( long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void futureRelationshipsReferenceRead( long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void futureNodePropertyReferenceRead( long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void futureRelationshipPropertyReferenceRead( long reference )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    public void block( long reference, ByteBlockCursor cursor )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    public void shutdown()
    {
        IllegalStateException failure = null;
        for ( StoreFile file : new StoreFile[] {nodes} )
        {
            try
            {
                file.close();
            }
            catch ( Exception e )
            {
                if ( failure == null )
                {
                    failure = new IllegalStateException( "Failed to close store files." );
                }
                failure.addSuppressed( e );
            }
        }
        if ( failure != null )
        {
            throw failure;
        }
    }

    static int nextPowerOfTwo( int v )
    {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    static long combineReference( long base, long modifier )
    {
        return modifier == 0 && base == INTEGER_MINUS_ONE ? -1 : base | modifier;
    }

    static int lcm( int a, int b )
    {
        return (a / gcd( a, b )) * b;
    }

    private static int gcd( int a, int b )
    {
        return a == b ? a : a > b ? gcd( a - b, b ) : gcd( a, b - a );
    }

    public CursorFactory cursorFactory()
    {
        return new CursorFactory( this );
    }

    int dynamicStoreRecordSize()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }
}
