package org.neo4j.impl.transaction;


/**
 * Defines the transaction isolation levels suported by Neo.
 */
public enum TransactionIsolationLevel
{
	/**
	 * Release read locks after operation completes but holds write locks
	 * untill transaction committs.
	 */
	READ_COMMITTED,
	/**
	 * Holds both read and write locks until transaction committs.
	 */
	BAD
}