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
package org.neo4j.kernel.impl.transaction;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.SystemException;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.Factory;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.KernelEventHandlers;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.KernelPanicEventGenerator;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.transaction.xaframework.TxIdGenerator;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.XaFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.BufferingLogging;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import static org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies.NO_PRUNING;
import static org.neo4j.kernel.impl.transaction.xaframework.RecoveryVerifier.ALWAYS_VALID;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class TxManagerTest
{
    @Test
    public void settingTmNotOkShouldAttachCauseToSubsequentErrors() throws Exception
    {
        // Given
        XaDataSourceManager mockXaManager = mock( XaDataSourceManager.class );
        File txLogDir = TargetDirectory.forTest( fs.get(), getClass() ).cleanDirectory( "log" );
        KernelHealth kernelHealth = new KernelHealth( panicGenerator, logging );
        TxManager txm = new TxManager( txLogDir, mockXaManager, DEV_NULL, fs.get(), null, null,
                kernelHealth, monitors );
        txm.doRecovery(); // Make the txm move to an ok state

        String msg = "These kinds of throwables, breaking our transaction managers, are why we can't have nice things.";

        // When
        txm.setTmNotOk( new Throwable( msg ) );

        // Then
        try
        {
            txm.begin();
            fail( "Should have thrown SystemException." );
        }
        catch ( SystemException topLevelException )
        {
            assertThat( "TM should forward a cause.", topLevelException.getCause(), is( Throwable.class ) );
            assertThat( "Cause should be the original cause", topLevelException.getCause().getMessage(), is( msg ) );
        }
    }

    @Test
    public void shouldNotSetTmNotOKForFailureInCommitted() throws Throwable
    {
        /*
         * I.e. when the commit has been done and the TxIdGenerator#committed method is called and fails,
         * it should not put the TM in not OK state. However that exception should still be propagated to
         * the user.
         */

        // GIVEN
        File directory = TargetDirectory.forTest( fs.get(), getClass() ).cleanDirectory( "dir" );
        TransactionStateFactory stateFactory = new TransactionStateFactory( logging );
        TxIdGenerator txIdGenerator = mock( TxIdGenerator.class );
        doThrow( RuntimeException.class ).when( txIdGenerator )
                .committed( any( XaDataSource.class ), anyInt(), anyLong(), any( Integer.class ) );
        stateFactory.setDependencies( mock( LockManager.class ),
                mock( NodeManager.class ), mock( RemoteTxHook.class ), txIdGenerator );
        XaDataSourceManager xaDataSourceManager = life.add( new XaDataSourceManager( DEV_NULL ) );
        KernelHealth kernelHealth = new KernelHealth( panicGenerator, logging );
        AbstractTransactionManager txManager = life.add( new TxManager( directory, xaDataSourceManager,
                logging.getMessagesLog( TxManager.class ), fs.get(), stateFactory, xidFactory, kernelHealth, monitors ) );
        XaFactory xaFactory = new XaFactory( new Config(), txIdGenerator, txManager, fs.get(), monitors,
                logging, ALWAYS_VALID, NO_PRUNING, kernelHealth );
        DummyXaDataSource dataSource = new DummyXaDataSource( UTF8.encode( "0xDDDDDE" ), "dummy", xaFactory,
                stateFactory, new File( directory, "log" ) );
        xaDataSourceManager.registerDataSource( dataSource );
        life.start();
        txManager.doRecovery();

        // WHEN
        txManager.begin();
        dataSource.getXaConnection().enlistResource( txManager.getTransaction() );
        txManager.commit();

        // THEN tx manager should still work here
        assertThat( logging.toString(), containsString( "Commit notification failed" ) );
        doNothing().when( txIdGenerator )
                .committed( any( XaDataSource.class ), anyInt(), anyLong(), any( Integer.class ) );
        txManager.begin();
        txManager.rollback();
        // and of course kernel should be healthy
        kernelHealth.assertHealthy( AssertionError.class );
    }

    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final KernelPanicEventGenerator panicGenerator = new KernelPanicEventGenerator(
            new KernelEventHandlers(StringLogger.DEV_NULL) );
    private final Monitors monitors = new Monitors();
    private final Logging logging = new BufferingLogging();
    private final Factory<byte[]> xidFactory = new Factory<byte[]>()
    {
        private final AtomicInteger id = new AtomicInteger();

        @Override
        public byte[] newInstance()
        {
            return ("test" + id.incrementAndGet()).getBytes();
        }
    };
    private final LifeSupport life = new LifeSupport();

    @After
    public void after()
    {
        life.shutdown();
    }
}
