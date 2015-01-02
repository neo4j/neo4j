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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Test;

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.StoreFileChannel;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestXaLogicalLogFiles {

    @Test
    public void shouldDetectLegacyLogs() throws Exception 
    {
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        when( fs.fileExists( new File( "logical_log.active" ) )).thenReturn( false );
        when(fs.fileExists(new File("logical_log"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log.1"))).thenReturn(false);
        when(fs.fileExists(new File("logical_log.2"))).thenReturn(false);
        XaLogicalLogFiles files = new XaLogicalLogFiles(new File("logical_log"), fs);
        
        assertThat(files.determineState(), is(XaLogicalLogFiles.State.LEGACY_WITHOUT_LOG_ROTATION));
    }

    @Test
    public void shouldDetectNoActiveFile() throws Exception 
    {
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        when(fs.fileExists(new File("logical_log.active"))).thenReturn(false);
        when(fs.fileExists(new File("logical_log"))).thenReturn(false);
        when(fs.fileExists(new File("logical_log.1"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log.2"))).thenReturn(false);
        XaLogicalLogFiles files = new XaLogicalLogFiles(new File("logical_log"), fs);
        
        assertThat(files.determineState(), is(XaLogicalLogFiles.State.NO_ACTIVE_FILE));
    }

    @Test
    public void shouldDetectLog1Active() throws Exception 
    {    
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        when(fs.fileExists(new File("logical_log.active"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log"))).thenReturn(false);
        when(fs.fileExists(new File("logical_log.1"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log.2"))).thenReturn(false);
        
        StoreChannel fc = mockedStoreChannel( XaLogicalLogTokens.LOG1 );
        when(fs.open(eq(new File("logical_log.active")), anyString())).thenReturn( fc );
        
        XaLogicalLogFiles files = new XaLogicalLogFiles(new File("logical_log"), fs);
        
        assertThat(files.determineState(), is(XaLogicalLogFiles.State.LOG_1_ACTIVE));
    }

    @Test
    public void shouldDetectLog2Active() throws Exception 
    {    
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        when(fs.fileExists(new File("logical_log.active"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log"))).thenReturn(false);
        when(fs.fileExists(new File("logical_log.1"))).thenReturn(false);
        when(fs.fileExists(new File("logical_log.2"))).thenReturn(true);
        
        StoreChannel fc = mockedStoreChannel( XaLogicalLogTokens.LOG2 );
        when(fs.open(eq(new File("logical_log.active")), anyString())).thenReturn(fc);
        
        XaLogicalLogFiles files = new XaLogicalLogFiles(new File("logical_log"), fs);
        
        assertThat(files.determineState(), is(XaLogicalLogFiles.State.LOG_2_ACTIVE));
    }

    @Test
    public void shouldDetectCleanShutdown() throws Exception 
    {    
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        when(fs.fileExists(new File("logical_log.active"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log"))).thenReturn(false);
        when(fs.fileExists(new File("logical_log.1"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log.2"))).thenReturn(false);
        
        StoreChannel fc = mockedStoreChannel( XaLogicalLogTokens.CLEAN );
        when(fs.open(eq(new File("logical_log.active")), anyString())).thenReturn(fc);
        
        XaLogicalLogFiles files = new XaLogicalLogFiles(new File("logical_log"), fs);
        
        assertThat(files.determineState(), is(XaLogicalLogFiles.State.CLEAN));
    }

    @Test
    public void shouldDetectDualLog1() throws Exception 
    {    
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        when(fs.fileExists(new File("logical_log.active"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log"))).thenReturn(false);
        when(fs.fileExists(new File("logical_log.1"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log.2"))).thenReturn(true);
        
        StoreChannel fc = mockedStoreChannel( XaLogicalLogTokens.LOG1 );
        when(fs.open(eq(new File("logical_log.active")), anyString())).thenReturn(fc);
        
        XaLogicalLogFiles files = new XaLogicalLogFiles(new File("logical_log"), fs);
        
        assertThat(files.determineState(), is(XaLogicalLogFiles.State.DUAL_LOGS_LOG_1_ACTIVE));
    }

    @Test
    public void shouldDetectDualLog2() throws Exception 
    {    
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        when(fs.fileExists(new File("logical_log.active"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log"))).thenReturn(false);
        when(fs.fileExists(new File("logical_log.1"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log.2"))).thenReturn(true);
        
        StoreChannel fc = mockedStoreChannel( XaLogicalLogTokens.LOG2 );
        when(fs.open(eq(new File("logical_log.active")), anyString())).thenReturn(fc);
        
        XaLogicalLogFiles files = new XaLogicalLogFiles(new File("logical_log"), fs);
        
        assertThat(files.determineState(), is(XaLogicalLogFiles.State.DUAL_LOGS_LOG_2_ACTIVE));
    }
    


    @Test(expected=IllegalStateException.class)
    public void shouldThrowIllegalStateExceptionOnUnrecognizedActiveContent() throws Exception 
    {    
        FileSystemAbstraction fs = mock(FileSystemAbstraction.class);
        when(fs.fileExists(new File("logical_log.active"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log"))).thenReturn(false);
        when(fs.fileExists(new File("logical_log.1"))).thenReturn(true);
        when(fs.fileExists(new File("logical_log.2"))).thenReturn(true);
        
        StoreChannel fc = mockedStoreChannel( ';' );
        when(fs.open(eq(new File("logical_log.active")), anyString())).thenReturn(fc);
        
        XaLogicalLogFiles files = new XaLogicalLogFiles(new File("logical_log"), fs);
        
        files.determineState();
    }
    
    private StoreChannel mockedStoreChannel( char c ) throws IOException
    {
        return new MockedFileChannel(ByteBuffer.allocate(4).putChar(c).array());
    }
    
    private static class MockedFileChannel extends StoreFileChannel
    {
        private ByteBuffer bs;

        public MockedFileChannel(byte [] bs) {
            super( (FileChannel) null );
            this.bs = ByteBuffer.wrap(bs);
        }

        @Override
        public long position() throws IOException
        {
            return bs.position();
        }

        @Override
        public int read(ByteBuffer buffer) throws IOException
        {
            int start = bs.position();
            buffer.put(bs);
            return bs.position() - start;
        }

        @Override
        public void close() throws IOException
        {
        }
    }
    
}
