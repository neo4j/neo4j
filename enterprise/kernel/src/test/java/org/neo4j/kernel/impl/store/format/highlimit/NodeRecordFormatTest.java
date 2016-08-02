package org.neo4j.kernel.impl.store.format.highlimit;


import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.kernel.impl.store.id.IdSequence;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NodeRecordFormatTest
{

    private NodeRecordFormat recordFormat;
    private StubPageCursor pageCursor;
    private TestIdSequence idSequence;

    @Before
    public void setUp()
    {
        recordFormat = new NodeRecordFormat();
        pageCursor = new StubPageCursor( 0, (int) ByteUnit.kibiBytes( 8 ) );
        idSequence = new TestIdSequence();
    }

    @After
    public void tearDown()
    {
        pageCursor.close();
    }

    @Test
    public void readWriteFixedReferencesRecord() throws Exception
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertTrue( "Record should use fixed reference format.", target.isUseFixedReferences() );
        assertEquals("Records should be equal.", source, target);
    }

    @Test
    public void useVariableLengthFormatWhenRelationshipIsMissing() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, Record.NULL_REFERENCE.byteValue(), 0L );

        writeReadRecord( source, target );

        assertFalse( "Record should use variable length reference format.", target.isUseFixedReferences() );
        assertEquals("Records should be equal.", source, target);
    }

    @Test
    public void useVariableLengthFormatWhenPropertyIsMissing() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, Record.NULL_REFERENCE.intValue(), true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertFalse( "Record should use variable length reference format.", target.isUseFixedReferences() );
        assertEquals("Records should be equal.", source, target);
    }

    @Test
    public void useVariableLengthFormatWhenRelationshipReferenceTooBig() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, 1L << 37, true, randomFixedReference(), 0L );

        writeReadRecord( source, target );

        assertFalse( "Record should use variable length reference format.", target.isUseFixedReferences() );
        assertEquals("Records should be equal.", source, target);
    }

    @Test
    public void useVariableLengthFormatWhenPropertyReferenceTooBig() throws IOException
    {
        NodeRecord source = new NodeRecord( 1 );
        NodeRecord target = new NodeRecord( 1 );
        source.initialize( true, randomFixedReference(), true, 1L << 37, 0L );

        writeReadRecord( source, target );

        assertFalse( "Record should use variable length reference format.", target.isUseFixedReferences() );
        assertEquals("Records should be equal.", source, target);
    }

    private void writeReadRecord( NodeRecord source, NodeRecord target ) throws java.io.IOException
    {
        int recordSize = NodeRecordFormat.RECORD_SIZE;
        recordFormat.prepare( source, recordSize, idSequence );
        recordFormat.write( source, pageCursor, recordSize );
        pageCursor.setOffset( 0 );
        recordFormat.read( target, pageCursor, RecordLoad.NORMAL, recordSize );
    }

    private long randomFixedReference()
    {
        return randomReference( 1L << (Integer.SIZE + (Byte.SIZE / 2)) );
    }

    private long randomReference( long maxValue )
    {
        return ThreadLocalRandom.current().nextLong( maxValue );
    }

    private static class TestIdSequence implements IdSequence
    {
        @Override
        public long nextId()
        {
            return -1;
        }
    }
}
