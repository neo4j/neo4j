package org.neo4j.kernel.api.index;

import java.util.Set;

public class DuplicateIndexEntryConflictException extends IndexEntryConflictException
{
    private final Object propertyValue;
    private final Set<Long> conflictingNodeIds;

    public DuplicateIndexEntryConflictException( Object propertyValue, Set<Long> conflictingNodeIds )
    {
        super( String.format( "Attempting to set same property value %s on nodes with ids %s " +
                "disallowed by unique index.", propertyValue, conflictingNodeIds ) );
        this.propertyValue = propertyValue;
        this.conflictingNodeIds = conflictingNodeIds;
    }

    public Object getPropertyValue()
    {
        return propertyValue;
    }

    public Set<Long> getConflictingNodeIds()
    {
        return conflictingNodeIds;
    }
}
