package org.neo4j.test.impl;

import java.util.Collections;
import java.util.List;

import org.junit.runners.model.MultipleFailureException;
import org.neo4j.helpers.Exceptions;

class JUnitMultipleExceptions extends MultipleExceptionsStrategy
{
    {
        // Make sure that we can access the JUnit MultipleFailureException type
        try
        {
            MultipleFailureException.assertEmpty( Collections.<Throwable>emptyList() );
        }
        catch ( Throwable e )
        {
            throw Exceptions.launderedException( e );
        }
    }

    @Override
    Throwable aggregate( List<Throwable> failures )
    {
        return new MultipleFailureException( failures );
    }
}
