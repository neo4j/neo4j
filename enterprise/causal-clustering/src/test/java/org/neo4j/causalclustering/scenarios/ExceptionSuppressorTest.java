package org.neo4j.causalclustering.scenarios;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ExceptionSuppressorTest
{
    @Test
    public void shouldNotCatchErrorsByDefault() throws Throwable
    {
        // given
        boolean suppressed = false;
        Error error = new Error();

        // when
        try ( ExceptionSuppressor suppressor = new ExceptionSuppressor() )
        {
            suppressor.execute( () -> {throw error;} );
            suppressed = true;
        }
        catch ( Throwable e )
        {
            assertEquals( error, e );
        }
        assertFalse( suppressed );
    }

    @Test
    public void shouldNotCatchThrowableByDefault() throws Throwable
    {
        // given
        boolean suppressed = false;
        Throwable throwable = new Throwable();

        // when
        try ( ExceptionSuppressor suppressor = new ExceptionSuppressor() )
        {
            suppressor.execute( () -> {throw throwable;} );
            suppressed = true;
        }
        catch ( Throwable e )
        {
            assertEquals( throwable, e );
        }
        assertFalse( suppressed );
    }

    @Test
    public void shouldCatchExceptionsByDefault() throws Exception
    {
        // given
        boolean suppressed = false;
        Exception exception = new Exception();

        // when
        try ( ExceptionSuppressor suppressor = new ExceptionSuppressor() )
        {
            suppressor.execute( () -> {throw exception;} );
            suppressed = true;
        }
        catch ( Throwable e )
        {
            assertEquals( exception, e );
        }
        assertTrue( suppressed );
    }

    @Test
    public void shouldCatchThrowableWhenConfigured() throws Exception
    {
        // given
        boolean suppressed = false;
        Throwable throwable = new Throwable();

        // when
        try ( ExceptionSuppressor suppressor = new ExceptionSuppressor( Throwable.class ) )
        {
            suppressor.execute( () -> {throw throwable;} );
            suppressed = true;
        }
        catch ( Throwable e )
        {
            assertEquals( throwable, e.getCause() );
        }
        assertTrue( suppressed );
    }

    @Test
    public void shouldImmediatelyThrowUnsuppressedDefault() throws Exception
    {
        // given
        boolean suppressed = false;
        IllegalArgumentException iae = new IllegalArgumentException();

        // when
        try ( ExceptionSuppressor suppressor = new ExceptionSuppressor( IllegalStateException.class ) )
        {
            suppressor.execute( () -> {throw iae;} );
            suppressed = true;
        }
        catch ( Throwable e )
        {
            assertEquals( iae, e );
        }
        assertFalse( suppressed );
    }

    @Test
    public void shouldSuppressMultipleExceptions() throws Exception
    {
        // given
        boolean suppressed = false;
        Exception ex1 = new Exception();
        Exception ex2 = new Exception();
        Exception ex3 = new Exception();

        // when
        try ( ExceptionSuppressor suppressor = new ExceptionSuppressor() )
        {
            suppressor.execute( () -> {throw ex1;} );
            suppressor.execute( () -> {throw ex2;} );
            suppressor.execute( () -> {throw ex3;} );
            suppressed = true;
        }
        catch ( Throwable e )
        {
            assertEquals( ex1, e );
            assertArrayEquals( new Exception[]{ex2, ex3}, e.getSuppressed() );
        }
        assertTrue( suppressed );
    }
}
