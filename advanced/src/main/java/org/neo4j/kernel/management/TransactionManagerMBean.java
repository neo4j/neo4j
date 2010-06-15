package org.neo4j.kernel.management;

public interface TransactionManagerMBean
{
    final String NAME = "Transactions";

    int getNumberOfOpenTransactions();

    int getPeakNumberOfConcurrentTransactions();

    int getNumberOfOpenedTransactions();

    long getNumberOfCommittedTransactions();

    long getNumberOfRollbackedTransactions();
}
