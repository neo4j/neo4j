/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import javax.transaction.xa.Xid;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.listeners.InvocationListener;
import org.mockito.listeners.MethodInvocationReport;
import org.mockito.stubbing.Answer;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.XidImpl;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalLogFile.Monitor;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.FailureOutput;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import static org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies.NO_PRUNING;

@Ignore
//TODO 2.2-future some tests here may be a good idea to reimplement.
public class XaLogicalLogTest
{
    @Rule
    public final FailureOutput output = new FailureOutput();
    private static final byte[] RESOURCE_ID = new byte[]{0x00, (byte) 0x99, (byte) 0xcc};
    private int reads;

    @Test
    public void shouldNotReadFromTheFileChannelWhenRotatingLog() throws Exception
    {
        // given
        // spy on the file system abstraction so that we can spy on the file channel for the logical log
        FileSystemAbstraction fs = spy( ephemeralFs.get() );
        File dir = TargetDirectory.forTest( fs, XaLogicalLogTest.class ).cleanDirectory( "log" );
        // -- when opening the logical log, spy on the file channel we return and count invocations to channel.read(*)
        when( fs.open( new File( dir, "logical.log.1" ), "rw" ) ).thenAnswer( new Answer<StoreChannel>()
        {
            @Override
            public StoreChannel answer( InvocationOnMock invocation ) throws Throwable
            {
                StoreFileChannel channel = (StoreFileChannel) invocation.callRealMethod();
                return mock( channel.getClass(), withSettings()
                        .spiedInstance( channel )
                        .name( "channel" )
                        .defaultAnswer( CALLS_REAL_METHODS )
                        .invocationListeners( new InvocationListener()
                        {
                            @Override
                            public void reportInvocation( MethodInvocationReport methodInvocationReport )
                            {
                                if ( methodInvocationReport.getInvocation().toString().startsWith( "channel.read(" ) )
                                {
                                    reads++;
                                }
                            }
                        } ) );
            }
        } );
        LifeSupport life = new LifeSupport();
        PhysicalLogFiles logFiles = new PhysicalLogFiles( dir, "logical.log", fs );
        PhysicalLogFile log = life.add( new PhysicalLogFile( fs, logFiles, 14/* <- This is the rotate threshold */,
        		NO_PRUNING, mock( TransactionIdStore.class ), mock( LogVersionRepository.class), mock( Monitor.class ),
        		mock( LogRotationControl.class), mock( TransactionMetadataCache.class ), mock ( Visitor.class ) ) );
        life.start();

        WritableLogChannel writer = log.getWriter();

        // -- set the log up with 10 transactions (with no commands, just start and commit)
        for ( int txId = 1; txId <= 10; txId++ )
        {
            writer.putLong( 100l );
        }

        // then
        assertEquals( 0, reads );
    }

    @Test
    public void shouldRespectCustomLogRotationThreshold() throws Exception
    {

    }

    @Test
    public void shouldDetermineHighestArchivedLogVersionFromFileNamesIfTheyArePresent() throws Exception
    {

    }

    @Test
    public void shouldNotPrepareAfterKernelPanicHasHappened() throws Exception
    {
        // This does not belong here
    }

    @Test
    public void shouldNotCommitOnePhaseAfterKernelPanicHasHappened() throws Exception
    {
    	// This does not belong here
    }

    @Test
    public void shouldNotCommitTwoPhaseAfterKernelPanicHasHappened() throws Exception
    {
    	// This does not belong here
    }

    public final @Rule EphemeralFileSystemRule ephemeralFs = new EphemeralFileSystemRule();
    public final Xid xid = new XidImpl( "global".getBytes(), "resource".getBytes() );
}
