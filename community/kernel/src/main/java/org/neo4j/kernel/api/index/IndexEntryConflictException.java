package org.neo4j.kernel.api.index;

import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public abstract class IndexEntryConflictException extends Exception
{
    public IndexEntryConflictException( String message )
    {
        super( message );
    }

    /**
     * Use this method in cases where {@link org.neo4j.kernel.api.index.IndexEntryConflictException} was caught but it should not have been
     * allowed to be thrown in the first place. Typically where the index we performed an operation on is not a
     * unique index.
     */
    public RuntimeException notAllowed( long labelId, long propertyKeyId )
    {
        return new IllegalStateException( String.format(
                "Index for label:%s propertyKey:%s should not require unique values.",
                labelId, propertyKeyId ), this );
    }

    public RuntimeException notAllowed( IndexDescriptor descriptor )
    {
        return notAllowed( descriptor.getLabelId(), descriptor.getPropertyKeyId() );
    }

    public abstract Object getPropertyValue();
}
