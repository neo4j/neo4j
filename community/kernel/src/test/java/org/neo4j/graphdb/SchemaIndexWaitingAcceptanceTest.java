package org.neo4j.graphdb;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.singleInstanceSchemaIndexProviderFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.api.index.ControlledPopulationSchemaIndexProvider;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.ImpermanentDatabaseRule;

public class SchemaIndexWaitingAcceptanceTest
{
    private final ControlledPopulationSchemaIndexProvider provider = new ControlledPopulationSchemaIndexProvider();

    @Rule
    public ImpermanentDatabaseRule rule = new ImpermanentDatabaseRule()
    {
        @Override
        protected void configure( GraphDatabaseFactory databaseFactory )
        {
            List<KernelExtensionFactory<?>> extensions = null;
            extensions = Arrays.<KernelExtensionFactory<?>>asList( singleInstanceSchemaIndexProviderFactory(
                    "test", provider ) );
            databaseFactory.addKernelExtensions( extensions );
        };
    };

    @Test
    public void shouldTimeoutWatingForIndexToComeOnline() throws Exception
    {
        // given
        GraphDatabaseService db = rule.getGraphDatabaseService();
        DoubleLatch latch = provider.installPopulationJobCompletionLatch();

        Transaction tx = db.beginTx();
        IndexDefinition index = db.schema().indexCreator( DynamicLabel.label( "Person" ) ).on( "name" ).create();
        tx.success();
        tx.finish();

        latch.awaitStart();

        // when
        try
        {
            // then
            db.schema().awaitIndexOnline( index, 1, TimeUnit.MILLISECONDS );

            fail( "Expected IllegalStateException to be thrown" );
        }
        catch ( IllegalStateException e )
        {
            // good
            assertThat( e.getMessage(), containsString( "come online" ) );
        }
        finally
        {
            latch.finish();
        }
    }

    @Test
    public void shouldTimeoutWatingForAllIndexesToComeOnline() throws Exception
    {
        // given
        GraphDatabaseService db = rule.getGraphDatabaseService();
        DoubleLatch latch = provider.installPopulationJobCompletionLatch();

        Transaction tx = db.beginTx();
        db.schema().indexCreator( DynamicLabel.label( "Person" ) ).on( "name" ).create();
        tx.success();
        tx.finish();

        latch.awaitStart();

        // when
        try
        {
            // then
            db.schema().awaitIndexesOnline( 1, TimeUnit.MILLISECONDS );

            fail( "Expected IllegalStateException to be thrown" );
        }
        catch ( IllegalStateException e )
        {
            // good
            assertThat( e.getMessage(), containsString( "come online" ) );
        }
        finally
        {
            latch.finish();
        }
    }
}
