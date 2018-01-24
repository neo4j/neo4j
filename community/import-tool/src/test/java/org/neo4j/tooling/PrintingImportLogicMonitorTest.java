package org.neo4j.tooling;

import org.junit.After;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.neo4j.unsafe.impl.batchimport.ImportLogic;

import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.io.ByteUnit.gibiBytes;

/**
 * Why test a silly thing like this? This implementation contains some printf calls that needs to get arguments correct
 * or will otherwise throw exception. It's surprisingly easy to get those wrong.
 */
public class PrintingImportLogicMonitorTest
{
    private final ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
    private final PrintStream out = new PrintStream( outBuffer );
    private final ByteArrayOutputStream errBuffer = new ByteArrayOutputStream();
    private final PrintStream err = new PrintStream( errBuffer );
    private final ImportLogic.Monitor monitor = new PrintingImportLogicMonitor( out, err );

    @After
    public void after()
    {
        System.out.println( "out " + outBuffer );
        System.out.println( "err " + errBuffer );
    }

    @Test
    public void mayExceedNodeIdCapacity() throws Exception
    {
        // given
        long capacity = 10_000_000;
        long estimatedCount = 12_000_000;

        // when
        monitor.mayExceedNodeIdCapacity( capacity, estimatedCount );

        // then
        String text = errBuffer.toString();
        assertTrue( text.contains( "WARNING" ) );
        assertTrue( text.contains( "exceed" ) );
        assertTrue( text.contains( String.valueOf( capacity ) ) );
        assertTrue( text.contains( String.valueOf( estimatedCount ) ) );
    }

    @Test
    public void mayExceedRelationshipIdCapacity() throws Exception
    {
        // given
        long capacity = 10_000_000;
        long estimatedCount = 12_000_000;

        // when
        monitor.mayExceedRelationshipIdCapacity( capacity, estimatedCount );

        // then
        String text = errBuffer.toString();
        assertTrue( text.contains( "WARNING" ) );
        assertTrue( text.contains( "exceed" ) );
        assertTrue( text.contains( String.valueOf( capacity ) ) );
        assertTrue( text.contains( String.valueOf( estimatedCount ) ) );
    }

    @Test
    public void insufficientHeapSize() throws Exception
    {
        // given
        long optimalHeapSize = gibiBytes( 2 );
        long heapSize = gibiBytes( 1 );

        // when
        monitor.insufficientHeapSize( optimalHeapSize, heapSize );

        // then
        String text = errBuffer.toString();
        assertTrue( text.contains( "WARNING" ) );
        assertTrue( text.contains( "too small" ) );
        assertTrue( text.contains( bytes( heapSize ) ) );
        assertTrue( text.contains( bytes( optimalHeapSize ) ) );
    }

    @Test
    public void abundantHeapSize() throws Exception
    {
        // given
        long optimalHeapSize = gibiBytes( 2 );
        long heapSize = gibiBytes( 10 );

        // when
        monitor.abundantHeapSize( optimalHeapSize, heapSize );

        // then
        String text = errBuffer.toString();
        assertTrue( text.contains( "WARNING" ) );
        assertTrue( text.contains( "unnecessarily large" ) );
        assertTrue( text.contains( bytes( heapSize ) ) );
        assertTrue( text.contains( bytes( optimalHeapSize ) ) );
    }

    @Test
    public void insufficientAvailableMemory() throws Exception
    {
        // given
        long estimatedCacheSize = gibiBytes( 2 );
        long optimalHeapSize = gibiBytes( 2 );
        long availableMemory = gibiBytes( 1 );

        // when
        monitor.insufficientAvailableMemory( estimatedCacheSize, optimalHeapSize, availableMemory );

        // then
        String text = errBuffer.toString();
        assertTrue( text.contains( "WARNING" ) );
        assertTrue( text.contains( "may not be sufficient" ) );
        assertTrue( text.contains( bytes( estimatedCacheSize ) ) );
        assertTrue( text.contains( bytes( optimalHeapSize ) ) );
        assertTrue( text.contains( bytes( availableMemory ) ) );
    }
}
