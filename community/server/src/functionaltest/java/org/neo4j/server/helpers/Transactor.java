package org.neo4j.server.helpers;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

public class Transactor
{

    private final org.neo4j.server.helpers.UnitOfWork unitOfWork;
    private final GraphDatabaseService graphDb;

    public Transactor( GraphDatabaseService graphDb, UnitOfWork unitOfWork )
    {
        this.unitOfWork = unitOfWork;
        this.graphDb = graphDb;
    }

    public void execute()
    {
        Transaction tx = graphDb.beginTx();

        try
        {
            unitOfWork.doWork();
            tx.success();
        }
        finally
        {
            tx.finish();
        }

    }

}
