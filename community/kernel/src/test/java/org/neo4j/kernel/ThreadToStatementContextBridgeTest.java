package org.neo4j.kernel;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;

public class ThreadToStatementContextBridgeTest
{
    @Test(expected = NotInTransactionException.class)
    public void shouldThrowNotInTransactionExceptionWhenNotInTransaction() throws Exception
    {
        // Given
        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge(null);

        // When
        bridge.getCtxForWriting();
    }

    @Test(expected = NotInTransactionException.class)
    public void shouldClearStateProperly() throws Exception
    {
        // Given
        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge(null);
        bridge.setTransactionContextForThread( mock(TransactionContext.class) );

        // When
//        bridge();
//        bridge.getCtxForWriting();
    }

    @Test
    public void shouldCreateStatementContextFromGivenTransactionContext() throws Exception
    {
        // Given
        TransactionContext mockedTxContext = mock( TransactionContext.class );

        StatementContext mockedStatementCtx = mock( StatementContext.class );
        when( mockedTxContext.newStatementContext() ).thenReturn(  mockedStatementCtx );

        ThreadToStatementContextBridge bridge = new ThreadToStatementContextBridge( null );
        bridge.setTransactionContextForThread( mockedTxContext );

        // When
        StatementContext ctx = bridge.getCtxForWriting();

        // Then
        verify( mockedTxContext ).newStatementContext();
        assertEquals("Should have returned the expected statement context", mockedStatementCtx, ctx);
    }

}
