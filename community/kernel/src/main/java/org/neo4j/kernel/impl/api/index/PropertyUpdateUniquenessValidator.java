package org.neo4j.kernel.impl.api.index;

import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.api.index.DuplicateIndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PreexistingIndexEntryConflictException;
import org.neo4j.kernel.impl.api.DiffSets;

public class PropertyUpdateUniquenessValidator
{
    private PropertyUpdateUniquenessValidator()
    {
    }

    public interface Lookup
    {
        Long currentlyIndexedNode( Object value ) throws IOException;
    }

    public static void validateUniqueness( Iterable<NodePropertyUpdate> updates, Lookup lookup )
            throws IndexEntryConflictException, IOException
    {
        Map<Object, DiffSets<Long>> referenceCount = new HashMap<Object, DiffSets<Long>>();

        for ( NodePropertyUpdate update : updates )
        {
            switch ( update.getUpdateMode() )
            {
                case ADDED:
                    propertyValueDiffSet( referenceCount, update.getValueAfter() ).add( update.getNodeId() );
                    break;
                case CHANGED:
                    propertyValueDiffSet( referenceCount, update.getValueBefore() ).remove( update.getNodeId() );
                    propertyValueDiffSet( referenceCount, update.getValueAfter() ).add( update.getNodeId() );
                    break;
                case REMOVED:
                    propertyValueDiffSet( referenceCount, update.getValueBefore() ).remove( update.getNodeId() );
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }

        for ( Map.Entry<Object, DiffSets<Long>> entry : referenceCount.entrySet() )
        {
            Object value = entry.getKey();
            int delta = entry.getValue().delta();
            if ( delta > 1 )
            {
                throw new DuplicateIndexEntryConflictException( value, asSet( entry.getValue().getAdded() ) );
            }
            if ( delta == 1 )
            {
                Long addedNode = single( entry.getValue().getAdded() );

                Long existingNode = lookup.currentlyIndexedNode( value );

                if ( existingNode != null && !addedNode.equals( existingNode ) )
                {
                    throw new PreexistingIndexEntryConflictException( value, existingNode, addedNode );
                }
            }
        }
    }

    private static DiffSets<Long> propertyValueDiffSet( Map<Object, DiffSets<Long>> referenceCount, Object value )
    {
        DiffSets<Long> diffSets = referenceCount.get( value );
        if ( diffSets == null )
        {
            referenceCount.put( value, diffSets = new DiffSets<Long>() );
        }
        return diffSets;
    }
}
