package org.neo4j.graphdb;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.ResourceClosingIterator.newResourceIterator;

public class ResourceIterableTest
{
    @Test
    public void streamShouldCloseOnCompleted() throws Throwable
    {
        // Given
        AtomicBoolean closed = new AtomicBoolean( false );
        ResourceIterator<Integer> resourceIterator = newResourceIterator( () -> closed.set( true ), iterator( new Integer[]{1, 2, 3} ) );

        ResourceIterable<Integer> iterable = () -> resourceIterator;

        // When
        List<Integer> result = iterable.stream().collect( Collectors.toList() );

        // Then
        assertEquals( asList(1,2,3), result );
        assertTrue( closed.get() );
    }
}