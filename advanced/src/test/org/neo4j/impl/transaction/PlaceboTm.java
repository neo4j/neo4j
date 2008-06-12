package org.neo4j.impl.transaction;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

class PlaceboTm implements TransactionManager 
{
	public void begin() throws NotSupportedException, SystemException {
		// TODO Auto-generated method stub
		
	}

	public void commit() throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException, IllegalStateException, SystemException {
		// TODO Auto-generated method stub
		
	}

	public int getStatus() throws SystemException {
		// TODO Auto-generated method stub
		return 0;
	}

	public Transaction getTransaction() throws SystemException {
		// TODO Auto-generated method stub
		return null;
	}

	public void resume(Transaction arg0) throws InvalidTransactionException, IllegalStateException, SystemException {
		// TODO Auto-generated method stub
		
	}

	public void rollback() throws IllegalStateException, SecurityException, SystemException {
		// TODO Auto-generated method stub
		
	}

	public void setRollbackOnly() throws IllegalStateException, SystemException {
		// TODO Auto-generated method stub
		
	}

	public void setTransactionTimeout(int arg0) throws SystemException {
		// TODO Auto-generated method stub
		
	}

	public Transaction suspend() throws SystemException {
		// TODO Auto-generated method stub
		return null;
	}
}
