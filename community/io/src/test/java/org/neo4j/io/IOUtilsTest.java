package org.neo4j.io;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import org.neo4j.test.NestedThrowableMatcher;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@RunWith( MockitoJUnitRunner.class )
public class IOUtilsTest
{
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private AutoCloseable faultyClosable;
    @Mock
    private AutoCloseable goodClosable1;
    @Mock
    private AutoCloseable goodClosable2;

    @Test
    public void closeAllSilently() throws Exception
    {
        IOUtils.closeAllSilently( goodClosable1, faultyClosable, goodClosable2 );

        verify( goodClosable1 ).close();
        verify( goodClosable2 ).close();
        verify( faultyClosable ).close();
    }

    @Test
    public void closeAllAndRethrowException() throws Exception
    {
        doThrow( new IOException( "Faulty closable" ) ).when( faultyClosable ).close();

        expectedException.expect( IOException.class );
        expectedException.expectMessage( "Exception closing multiple resources" );
        expectedException.expect( new NestedThrowableMatcher( IOException.class ) );

        IOUtils.closeAll( goodClosable1, faultyClosable, goodClosable2 );
    }

}