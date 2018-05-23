package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.transaction.state.PropertyCreator;
import org.neo4j.kernel.impl.transaction.state.PropertyTraverser;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.unsafe.batchinsert.internal.DirectRecordAccessSet;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;

import static org.eclipse.collections.impl.block.factory.primitive.IntPredicates.alwaysFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.iterator;

public class RecordPropertyCursorTest
{
    @Rule
    public final PageCacheAndDependenciesRule storage = new PageCacheAndDependenciesRule();
    @Rule
    public final RandomRule random = new RandomRule().withConfiguration( new RandomValues.Default()
    {
        @Override
        public int stringMaxLength()
        {
            return 10_000;
        }
    } );
    private NeoStores neoStores;
    private PropertyCreator creator;
    private NodeRecord owner;

    @Before
    public void setup()
    {
        neoStores = new StoreFactory( storage.directory().graphDbDir(), Config.defaults(), new DefaultIdGeneratorFactory( storage.fileSystem() ),
                storage.pageCache(), storage.fileSystem(), NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY ).openAllNeoStores( true );
        creator = new PropertyCreator( neoStores.getPropertyStore(), new PropertyTraverser() );
        owner = neoStores.getNodeStore().newRecord();
    }

    @After
    public void closeStore()
    {
        neoStores.close();
    }

    @Test
    public void shouldReadPropertyChain()
    {
        // given
        Value[] values = createValues();
        long firstPropertyId = storeValuesAsPropertyChain( creator, owner, values );

        // when
        assertPropertyChain( values, firstPropertyId, createCursor() );
    }

    @Test
    public void shouldReuseCursor()
    {
        // given
        Value[] valuesA = createValues();
        long firstPropertyIdA = storeValuesAsPropertyChain( creator, owner, valuesA );
        Value[] valuesB = createValues();
        long firstPropertyIdB = storeValuesAsPropertyChain( creator, owner, valuesB );

        // then
        RecordPropertyCursor cursor = createCursor();
        assertPropertyChain( valuesA, firstPropertyIdA, cursor );
        assertPropertyChain( valuesB, firstPropertyIdB, cursor );
    }

    @Test
    public void closeShouldBeIdempotent()
    {
        // given
        RecordPropertyCursor cursor = createCursor();

        // when
        cursor.close();

        // then
        cursor.close();
    }

    private RecordPropertyCursor createCursor()
    {
        return new RecordPropertyCursor( neoStores.getPropertyStore() );
    }

    private void assertPropertyChain( Value[] values, long firstPropertyId, RecordPropertyCursor cursor )
    {
        Map<Integer,Value> expectedValues = asMap( values );
        cursor.init( firstPropertyId );
        while ( cursor.next( alwaysFalse() ) )
        {
            // then
            assertEquals( expectedValues.remove( cursor.propertyKey() ), cursor.propertyValue() );
        }
        assertTrue( expectedValues.isEmpty() );
    }

    private Value[] createValues()
    {
        int numberOfProperties = random.nextInt( 1, 20 );
        Value[] values = new Value[numberOfProperties];
        for ( int key = 0; key < numberOfProperties; key++ )
        {
            values[key] = random.nextValue();
        }
        return values;
    }

    private long storeValuesAsPropertyChain( PropertyCreator creator, NodeRecord owner, Value[] values )
    {
        DirectRecordAccessSet access = new DirectRecordAccessSet( neoStores );
        long firstPropertyId = creator.createPropertyChain( owner, blocksOf( creator, values ), access.getPropertyRecords() );
        access.close();
        return firstPropertyId;
    }

    private Map<Integer,Value> asMap( Value[] values )
    {
        Map<Integer,Value> map = new HashMap<>();
        for ( int key = 0; key < values.length; key++ )
        {
            map.put( key, values[key] );
        }
        return map;
    }

    private Iterator<PropertyBlock> blocksOf( PropertyCreator creator, Value[] values )
    {
        return new IteratorWrapper<PropertyBlock,Value>( iterator( values ) )
        {
            int key = 0;

            @Override
            protected PropertyBlock underlyingObjectToObject( Value value )
            {
                return creator.encodePropertyValue( key++, value );
            }
        };
    }
}
