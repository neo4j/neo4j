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
package org.neo4j.kernel.impl.newapi;

import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.opentest4j.AssertionFailedError;

import java.util.Collections;
import java.util.Optional;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.graphdb.security.AuthorizationViolationException;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.helpers.StubCursorFactory;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubRead;
import org.neo4j.internal.kernel.api.helpers.StubRelationshipCursor;
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.kernel.api.security.CommunitySecurityLog;
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.internal.schema.IndexProviderDescriptor;
import org.neo4j.internal.schema.LabelSchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorImplementation;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingProvidersService;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.index.schema.FulltextIndexProviderFactory;
import org.neo4j.kernel.impl.index.schema.GenericNativeIndexProvider;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.logging.FormattedLogFormat;
import org.neo4j.logging.Level;
import org.neo4j.logging.SecurityLogHelper;
import org.neo4j.logging.log4j.LogExtended;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.lock.LockTracer.NONE;
import static org.neo4j.logging.SecurityLogHelper.line;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.values.storable.Values.intValue;

abstract class OperationsTest
{
    protected static final long TOKEN_INDEX_RESOURCE_ID = Long.MAX_VALUE;

    protected final KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
    protected Operations operations;
    protected final Locks.Client locks = mock( Locks.Client.class );
    protected final Write write = mock( Write.class );
    protected InOrder order;
    protected FullAccessNodeCursor nodeCursor;
    protected FullAccessPropertyCursor propertyCursor;
    protected DefaultRelationshipScanCursor relationshipCursor;
    protected TransactionState txState;
    protected AllStoreHolder allStoreHolder;
    protected final LabelSchemaDescriptor schema = SchemaDescriptors.forLabel( 123, 456 );
    protected StorageReader storageReader;
    protected StorageSchemaReader storageReaderSnapshot;
    protected ConstraintIndexCreator constraintIndexCreator;
    protected IndexingService indexingService;
    protected TokenHolders tokenHolders;
    protected CommandCreationContext creationContext;
    protected SecurityLogHelper logHelper;
    protected CommunitySecurityLog securityLog;
    protected StorageLocks storageLocks;
    protected static final String DB_NAME = "db.test";
    private StoreCursors storeCursors;

    abstract FormattedLogFormat getFormat();

    @BeforeEach
    void setUp() throws Exception
    {
        TxState realTxState = new TxState();
        txState = Mockito.spy( realTxState );
        storeCursors = mock( StoreCursors.class );
        when( transaction.getReasonIfTerminated() ).thenReturn( Optional.empty() );
        when( transaction.lockClient() ).thenReturn( locks );
        when( transaction.dataWrite() ).thenReturn( write );
        when( transaction.isOpen() ).thenReturn( true );
        when( transaction.lockTracer() ).thenReturn( LockTracer.NONE );
        when( transaction.txState() ).thenReturn( txState );
        when( transaction.storeCursors() ).thenReturn( storeCursors );
        when( transaction.securityContext() ).thenReturn( SecurityContext.authDisabled( AccessMode.Static.FULL, EMBEDDED_CONNECTION, DB_NAME ) );
        logHelper = new SecurityLogHelper( getFormat() );
        securityLog = new CommunitySecurityLog( (LogExtended) logHelper.getLogProvider().getLog( this.getClass() ) );
        when( transaction.securityAuthorizationHandler() ).thenReturn( new SecurityAuthorizationHandler( securityLog ) );

        DefaultPooledCursors cursors = mock( DefaultPooledCursors.class );
        nodeCursor = mock( FullAccessNodeCursor.class );
        propertyCursor = mock( FullAccessPropertyCursor.class );
        relationshipCursor = mock( DefaultRelationshipScanCursor.class );
        when( cursors.allocateFullAccessNodeCursor( NULL_CONTEXT ) ).thenReturn( nodeCursor );
        when( cursors.allocateFullAccessPropertyCursor( NULL_CONTEXT, INSTANCE ) ).thenReturn( propertyCursor );
        when( cursors.allocateRelationshipScanCursor( NULL_CONTEXT ) ).thenReturn( relationshipCursor );
        StorageEngine engine = mock( StorageEngine.class );
        storageReader = mock( StorageReader.class );
        storageReaderSnapshot = mock( StorageSchemaReader.class );
        when( storageReader.nodeExists( anyLong(), any() ) ).thenReturn( true );
        when( storageReader.constraintsGetForLabel( anyInt() )).thenReturn( Collections.emptyIterator() );
        when( storageReader.constraintsGetAll() ).thenReturn( Collections.emptyIterator() );
        when( storageReader.schemaSnapshot() ).thenReturn( storageReaderSnapshot );
        when( engine.newReader() ).thenReturn( storageReader );
        when( engine.createStorageCursors( any() ) ).thenReturn( storeCursors );
        indexingService = mock( IndexingService.class );
        Dependencies dependencies = new Dependencies();
        var facade = mock( GraphDatabaseFacade.class );
        dependencies.satisfyDependency( facade );
        storageLocks = mock( StorageLocks.class );
        allStoreHolder = new AllStoreHolder( storageReader, transaction, storageLocks, cursors, mock( GlobalProcedures.class ),
                mock( SchemaState.class ), indexingService, mock( IndexStatisticsStore.class ), dependencies, INSTANCE );
        constraintIndexCreator = mock( ConstraintIndexCreator.class );
        tokenHolders = mockedTokenHolders();
        creationContext = mock( CommandCreationContext.class );
        IndexingProvidersService indexingProvidersService = mock( IndexingProvidersService.class );
        when( indexingProvidersService.indexProviderByName( "native-btree-1.0" ) ).thenReturn( GenericNativeIndexProvider.DESCRIPTOR );
        when( indexingProvidersService.getDefaultProvider() ).thenReturn( GenericNativeIndexProvider.DESCRIPTOR );
        when( indexingProvidersService.indexProviderByName( "fulltext-1.0" ) ).thenReturn( FulltextIndexProviderFactory.DESCRIPTOR );
        when( indexingProvidersService.getFulltextProvider() ).thenReturn( FulltextIndexProviderFactory.DESCRIPTOR );
        when( indexingProvidersService.indexProviderByName( "provider-1.0" ) ).thenReturn( new IndexProviderDescriptor( "provider", "1.0" ) );
        when( indexingProvidersService.completeConfiguration( any() ) ).thenAnswer( inv -> inv.getArgument( 0 ) );
        operations = new Operations( allStoreHolder, storageReader, mock( IndexTxStateUpdater.class ), creationContext, storageLocks,
                transaction, new KernelToken( storageReader, creationContext, transaction, tokenHolders ), cursors,
                constraintIndexCreator, mock( ConstraintSemantics.class ), indexingProvidersService, Config.defaults(), INSTANCE,
                () -> KernelVersion.LATEST, mock( DbmsRuntimeRepository.class ) );
        operations.initialize( NULL_CONTEXT );

        this.order = inOrder( locks, txState, storageReader, storageReaderSnapshot, creationContext, storageLocks );
    }

    @AfterEach
    void tearDown()
    {
        operations.release();
    }

    @Test
    void nodeAddLabelShouldFailReadOnly() throws Exception
    {
        String message = runForSecurityLevel(() -> operations.nodeAddLabel(1L, 2),  AccessMode.Static.READ, false);
        String expected = String.format( "Set label for label 'Label' on database '%s' is not allowed for AUTH_DISABLED with READ.", DB_NAME );
        assertThat( message ).contains( expected );
        logHelper.assertLog( getFormat() ).containsOrdered(
                line().level( Level.ERROR )
                      .database( DB_NAME )
                      .source( ClientConnectionInfo.EMBEDDED_CONNECTION.asConnectionDetails() )
                      .message( expected )
        );
    }

    @Test
    void nodeAddLabelShouldFailAccess() throws Exception
    {
        String message = runForSecurityLevel(() -> operations.nodeAddLabel(1L, 2),  AccessMode.Static.ACCESS, false);
        String expected = String.format( "Set label for label 'Label' on database '%s' is not allowed for AUTH_DISABLED with ACCESS.", DB_NAME );
        assertThat( message).contains( expected );
        logHelper.assertLog( getFormat() ).containsOrdered(
                line().level( Level.ERROR )
                      .database( DB_NAME )
                      .source( ClientConnectionInfo.EMBEDDED_CONNECTION.asConnectionDetails() )
                      .message( expected )
        );
    }

    @Test
    void nodeRemoveLabelShouldFailReadOnly() throws Exception
    {
        String message = runForSecurityLevel(() -> operations.nodeRemoveLabel(1L, 3),  AccessMode.Static.READ, false);
        String expected = String.format( "Remove label for label 'Label' on database '%s' is not allowed for AUTH_DISABLED with READ.", DB_NAME );
        assertThat( message).contains( expected );
        logHelper.assertLog( getFormat() ).containsOrdered(
                line().level( Level.ERROR )
                      .database( DB_NAME )
                      .source( ClientConnectionInfo.EMBEDDED_CONNECTION.asConnectionDetails() )
                      .message( expected )
        );
    }

    @Test
    void nodeRemoveLabelShouldFailAccess() throws Exception
    {
        String message = runForSecurityLevel(() -> operations.nodeRemoveLabel(1L, 3),  AccessMode.Static.ACCESS, false);
        String expected = String.format( "Remove label for label 'Label' on database '%s' is not allowed for AUTH_DISABLED with ACCESS.", DB_NAME );
        assertThat( message).contains( expected );
        logHelper.assertLog( getFormat() ).containsOrdered(
                line().level( Level.ERROR )
                      .database( DB_NAME )
                      .source( ClientConnectionInfo.EMBEDDED_CONNECTION.asConnectionDetails() )
                      .message( expected )
        );
    }

    @Test
    void nodeApplyChangesShouldLockNodeAndLabels() throws Exception
    {
        // given
        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.labels() ).thenReturn( Labels.from( 1, 2 ) );
        long node = 1;

        // when
        operations.nodeApplyChanges( node, IntSets.immutable.of( 3 ), IntSets.immutable.of( 1 ), IntObjectMaps.immutable.of( 1, intValue( 10 )) );

        // then
        verify( locks ).acquireExclusive( any(), eq( ResourceTypes.NODE ), eq( 1L )  );
        verify( locks ).acquireShared( any(), eq( ResourceTypes.LABEL ), eq( 1L )  );
        verify( locks ).acquireShared( any(), eq( ResourceTypes.LABEL ), eq( 3L )  );
        verify( locks ).acquireShared( any(), eq( ResourceTypes.LABEL ), eq( SchemaDescriptorImplementation.TOKEN_INDEX_LOCKING_ID ) );
        verify( locks ).acquireShared( any(), eq( ResourceTypes.LABEL ), eq( 1L ), eq( 2L ) );
        verify( storageLocks ).acquireNodeLabelChangeLock( any(), eq( node ), eq( 1 ) );
        verify( storageLocks ).acquireNodeLabelChangeLock( any(), eq( node ), eq( 3 ) );
    }

    @Test
    void relationshipApplyChangesShouldLockRelationshipAndType() throws Exception
    {
        // given
        int type = 5;
        when( relationshipCursor.next() ).thenReturn( true );
        when( relationshipCursor.type() ).thenReturn( type );
        long relationship = 1;

        // when
        operations.relationshipApplyChanges( relationship, IntObjectMaps.immutable.of( 1, intValue( 10 ) ) );

        // then
        verify( locks ).acquireExclusive( any(), eq( ResourceTypes.RELATIONSHIP ), eq( 1L )  );
        verify( locks ).acquireShared( any(), eq( ResourceTypes.RELATIONSHIP_TYPE ), eq( (long) type )  );
    }

    protected String runForSecurityLevel( Executable executable, AccessMode mode, boolean shoudldBeAuthorized ) throws Exception
    {
        SecurityContext securityContext = SecurityContext.authDisabled( mode, ClientConnectionInfo.EMBEDDED_CONNECTION, DB_NAME );
        when( transaction.securityContext() ).thenReturn( securityContext );
        when( transaction.securityAuthorizationHandler() ).thenReturn( new SecurityAuthorizationHandler( securityLog ) );

        when( nodeCursor.next() ).thenReturn( true );
        when( nodeCursor.hasLabel( 2 ) ).thenReturn( false );
        when( nodeCursor.hasLabel( 3 ) ).thenReturn( true );
        when( tokenHolders.labelTokens().getTokenById( anyInt() ) ).thenReturn( new NamedToken( "Label", 2 ) );
        if ( shoudldBeAuthorized )
        {
            assertAuthorized( executable );
            return null;
        }
        else
        {
            AuthorizationViolationException exception = assertThrows( AuthorizationViolationException.class, executable );
            return exception.getMessage();
        }
    }

    private static void assertAuthorized( Executable executable )
    {
        try
        {
            executable.execute();
        }
        catch ( AuthorizationViolationException e )
        {
            throw new AssertionFailedError( e.getMessage(), e );
        }
        catch ( EntityNotFoundException e )
        {
            // Don't care about this
        }
        catch ( Throwable t )
        {
            throw new AssertionFailedError( "Unexpected exception thrown: " + t.getMessage(), t );
        }
    }

    private static TokenHolders mockedTokenHolders()
    {
        return new TokenHolders(
                mock( TokenHolder.class ),
                mock( TokenHolder.class ),
                mock( TokenHolder.class ) );
    }

    public static void returnRelationships( KernelTransactionImplementation ktx, final TestRelationshipChain relIds )
    {
        StubRead read = new StubRead();
        when( ktx.dataRead() ).thenReturn( read );
        StubCursorFactory cursorFactory = new StubCursorFactory( true );
        cursorFactory.withRelationshipTraversalCursors( new StubRelationshipCursor( relIds ) );

        when( ktx.lockTracer() ).thenReturn( NONE );
        when( ktx.cursors() ).thenReturn( cursorFactory );
        when( ktx.ambientNodeCursor() ).thenAnswer( args -> new StubNodeCursor( false ).withNode( 42L ) );
    }
}
