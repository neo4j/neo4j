package org.neo4j.kernel.impl.index.schema;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.test.rule.DatabaseRule;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Values;

import static java.time.LocalDate.now;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.fail;
import static org.neo4j.test.TestLabels.LABEL_ONE;

public class IndexValueSupportIT
{
    private static final String KEY = "key";

    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule().withSetting( GraphDatabaseSettings.enable_native_schema_index, "false" );

    @Test
    public void shouldFailOnIndexingTemporalValueInUnsupportedIndex()
    {
        shouldFailOnIndexingTemporalValueInUnsupportedIndex( now() );
    }

    @Test
    public void shouldFailOnIndexingSpatialValueInUnsupportedIndex()
    {
        PointValue value = Values.pointValue( CoordinateReferenceSystem.WGS84, 2.0, 2.0 );
        shouldFailOnIndexingTemporalValueInUnsupportedIndex( value );
    }

    private void shouldFailOnIndexingTemporalValueInUnsupportedIndex( Object value )
    {
        // given
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL_ONE ).on( KEY ).create();
            tx.success();
        }
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().awaitIndexesOnline( 10, SECONDS );
            tx.success();
        }

        // when
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode( LABEL_ONE ).setProperty( KEY, value );
                tx.success();
            }
            fail( "Should have failed" );
        }
        catch ( TransactionFailureException e )
        {
            // then good
        }
    }
}
