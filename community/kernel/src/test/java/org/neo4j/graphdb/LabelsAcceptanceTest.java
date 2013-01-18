package org.neo4j.graphdb;

import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.ImpermanentDatabaseRule;

public class LabelsAcceptanceTest
{

    public @Rule
    ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private enum Labels implements Label {

        MY_LABEL;

    }

    @Test
    public void addingALabelUsingAValidIdentifierShouldSucceed() throws Exception
    {
        // Given
        GraphDatabaseService beansAPI = dbRule.getGraphDatabaseService();
        Node myNode = null;

        // When
        Transaction tx = beansAPI.beginTx();
        try
        {
            myNode = beansAPI.createNode();
            myNode.addLabel(Labels.MY_LABEL);

            tx.success();
        } finally
        {
            tx.finish();
        }

        // Then
        assertTrue( "Label should have been added to node", myNode.hasLabel( Labels.MY_LABEL ) );
    }

}
