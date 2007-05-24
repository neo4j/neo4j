package org.neo4j.impl.nioneo.store;

/**
 * Defines the two types of operations that can be made to an acquired 
 * {@link PersistenceWindow}. Operation is either <CODE>READ</CODE> or
 * <CODE>WRITE</CODE>.
 */
public enum OperationType
{
	READ, WRITE
}
