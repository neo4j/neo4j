package org.neo4j.kernel;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.junit.Test;


public class TestPlaceboTransaction
{

    @Test
    public void shouldRollbackParentByDefault() throws SystemException
    {
        // Given
        TransactionManager mockTxManager = mock( TransactionManager.class );
        Transaction mockTopLevelTx = mock( Transaction.class );
        
        when( mockTxManager.getTransaction() ).thenReturn( mockTopLevelTx );
        
        PlaceboTransaction placeboTx = new PlaceboTransaction( mockTxManager );
     
        // When
        placeboTx.finish();
        
        // Then
        verify( mockTopLevelTx ).setRollbackOnly();
        
    }

    @Test
    public void shouldRollbackParentIfFailureCalled() throws SystemException
    {
        // Given
        TransactionManager mockTxManager = mock( TransactionManager.class );
        Transaction mockTopLevelTx = mock( Transaction.class );
        
        when( mockTxManager.getTransaction() ).thenReturn( mockTopLevelTx );
        
        PlaceboTransaction placeboTx = new PlaceboTransaction( mockTxManager );
     
        // When
        placeboTx.failure();
        placeboTx.finish();
        
        // Then
        verify( mockTopLevelTx ).setRollbackOnly();
    }
    
    @Test
    public void shouldNotRollbackParentIfSuccessCalled() throws SystemException
    {
        // Given
        TransactionManager mockTxManager = mock( TransactionManager.class );
        Transaction mockTopLevelTx = mock( Transaction.class );
        
        when( mockTxManager.getTransaction() ).thenReturn( mockTopLevelTx );
        
        PlaceboTransaction placeboTx = new PlaceboTransaction( mockTxManager );
     
        // When
        placeboTx.success();
        placeboTx.finish();
        
        // Then
        verifyNoMoreInteractions( mockTopLevelTx );
    }
    
}
