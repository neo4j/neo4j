package org.neo4j.kernel.impl.locking;

import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class SimpleStatementLocksTest
{
    @Test
    public void shouldUseSameClientForImplicitAndExplicit() throws Exception
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final SimpleStatementLocks statementLocks = new SimpleStatementLocks( client );

        // THEN
        assertSame( client, statementLocks.explicit() );
        assertSame( client, statementLocks.implicit() );
    }

    @Test
    public void shouldDoNothingWithClientWhenPreparingForCommit() throws Exception
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final SimpleStatementLocks statementLocks = new SimpleStatementLocks( client );

        // WHEN
        statementLocks.prepareForCommit();

        // THEN
        verifyNoMoreInteractions( client );
    }

    @Test
    public void shouldStopUnderlyingClient() throws Exception
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final SimpleStatementLocks statementLocks = new SimpleStatementLocks( client );

        // WHEN
        statementLocks.stop();

        // THEN
        verify( client ).stop();
    }

    @Test
    public void shouldCloseUnderlyingClient() throws Exception
    {
        // GIVEN
        final Locks.Client client = mock( Locks.Client.class );
        final SimpleStatementLocks statementLocks = new SimpleStatementLocks( client );

        // WHEN
        statementLocks.close();

        // THEN
        verify( client ).close();
    }
}