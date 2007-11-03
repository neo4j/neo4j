package org.neo4j.impl.transaction;


/**
 * Defines the transaction isolation levels supported by Neo.
 */
public enum TransactionIsolationLevel
{
	/**
	 * Release read locks after operation completes but holds write locks
	 * until transaction commits.
	 */
	READ_COMMITTED,
	/**
	 * Holds both read and write locks until transaction commits.
	 */
	BAD
}