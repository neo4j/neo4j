package org.neo4j.kernel.impl.nioneo.xa;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class RecordChangesTest
{

    private RecordAccess.Loader<Object, Object, Object> loader = new RecordAccess.Loader<Object, Object, Object>()
    {
        @Override
        public Object newUnused( Object o, Object additionalData )
        {
            return o;
        }

        @Override
        public Object load( Object o, Object additionalData )
        {
            return o;
        }

        @Override
        public void ensureHeavy( Object o )
        {

        }

        @Override
        public Object clone( Object o )
        {
            return o.toString();
        }
    };

    @Test
    public void shouldCountChanges() throws Exception
    {
        // Given
        RecordChanges<Object, Object, Object> change = new RecordChanges<>( loader, false );

        // When
        change.getOrLoad( "K1", null ).forChangingData();
        change.getOrLoad( "K1", null ).forChangingData();
        change.getOrLoad( "K2", null ).forChangingData();
        change.getOrLoad( "K3", null ).forReadingData();

        // Then
        assertThat(change.changeSize(), equalTo(2));
    }

}
