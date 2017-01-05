package org.neo4j.kernel.impl.api.cursor;

import org.junit.Test;

import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.impl.util.Cursors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TxSingleNodeCursorTest
{

    private final TransactionState state = mock( TransactionState.class );
    private TxSingleNodeCursor cursor = new TxSingleNodeCursor( state, ( l ) ->
    {
    } );

    @Test
    public void shouldNotLoopForeverWhenNodesAreAddedToTheTxState() throws Exception
    {
        // given
        int nodeId = 42;
        when( state.nodeIsDeletedInThisTx( nodeId ) ).thenReturn( false );
        when( state.nodeIsAddedInThisTx( nodeId ) ).thenReturn( true );

        // when
        cursor.init( Cursors.empty(), nodeId );

        // then
        assertTrue( cursor.next() );
        assertFalse( cursor.next() );
    }
}
