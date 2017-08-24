package org.neo4j.index.internal.gbptree;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ConsistencyCheckerDeltaFormatTest extends ConsistencyCheckerTest
{
    // tests to add for delta format

    @Test
    public void shouldDetectOutOfOrderDeltaKeys() throws Exception
    {
        // given
        node.main().insertKeyAt( cursor, new MutableLong( 3 ), 0, 0 );
        node.main().insertKeyAt( cursor, new MutableLong( 5 ), 1, 1 );
        node.main().setKeyCount( cursor, 2 );
        node.delta().insertKeyAt( cursor, new MutableLong( 2 ), 0, 0 );
        node.delta().insertKeyAt( cursor, new MutableLong( 1 ), 1, 1 );
        node.delta().setKeyCount( cursor, 2 );

        // when
        try
        {
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Non-unique key 1" ) );
        }
    }

    @Test
    public void shouldDetectNonUniqueKeysBetweenMainAndDeltaSections() throws Exception
    {
        // given
        node.main().insertKeyAt( cursor, new MutableLong( 3 ), 0, 0 );
        node.main().insertKeyAt( cursor, new MutableLong( 5 ), 1, 1 );
        node.main().insertKeyAt( cursor, new MutableLong( 7 ), 2, 2 );
        node.main().setKeyCount( cursor, 3 );
        node.delta().insertKeyAt( cursor, new MutableLong( 4 ), 0, 0 );
        node.delta().insertKeyAt( cursor, new MutableLong( 5 ), 1, 1 );
        node.delta().setKeyCount( cursor, 2 );

        // when
        try
        {
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Non-unique key 5" ) );
        }
    }

    @Test
    public void shouldDetectHighestKeyNotInMain() throws Exception
    {
        // given
        node.main().insertKeyAt( cursor, new MutableLong( 3 ), 0, 0 );
        node.main().insertKeyAt( cursor, new MutableLong( 5 ), 1, 1 );
        node.main().insertKeyAt( cursor, new MutableLong( 7 ), 2, 2 );
        node.main().setKeyCount( cursor, 3 );
        node.delta().insertKeyAt( cursor, new MutableLong( 4 ), 0, 0 );
        node.delta().insertKeyAt( cursor, new MutableLong( 8 ), 1, 1 );
        node.delta().setKeyCount( cursor, 2 );

        // when
        try
        {
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Highest key 8 not in main section" ) );
        }
    }
}
