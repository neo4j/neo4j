package org.neo4j.index.btree;

public interface RangePredicate
{
    /**
     * Decide if a key is inside range, to the left of range or to the right of range
     * It is completely fine for a RangePredicate to only have one side (only ever return 0 or neg/pos value)
     * or always return 0 (consider all keys to be in range)
     *
     * @param key   long[] of length 2 where key[0] is id and key[1] is property value
     * @return      0 if inside range
     *              negative value if key is smaller than range
     *              positive if key is larger than range
     */
    int inRange( long[] key );

    /**
     * Demands match on id, accept all property values
     * @param id    id to match
     * @return      A new {@link index.btree.RangePredicate} representing a no limit range
     */
    public static RangePredicate noLimit( long id )
    {
        return key -> key[0] == id ? 0 : key[0] < id ? -1 : 1;
    }

    /**
     * No demands. Useful for scans.
     * @return      A new {@link index.btree.RangePredicate} that have no demands
     */
    public static RangePredicate acceptAll()
    {
        return key -> 0;
    }

    /**
     * Demands match on id and property val to be lower than prop
     * @param id    id to match
     * @param prop  property value to be lower than
     * @return      A new {@link RangePredicate} representing a lower than limit
     */
    public static RangePredicate lower( long id, long prop )
    {
        return key -> {
            if ( key[0] == id )
            {
                return key[1] < prop ? 0 : 1;
            }
            else
            {
                return key[0] < id ? -1 : 1;
            }
        };
    }

    /**
     * Demands match on id and property val to be lower than or equal to prop
     * @param id    id to match
     * @param prop  property value to be lower than or equal to
     * @return      A new {@link RangePredicate} representing a lower than or equal to limit
     */
    public static RangePredicate lowerOrEqual( long id, long prop )
    {
        return key -> {
            if ( key[0] == id )
            {
                return key[1] <= prop ? 0 : 1;
            }
            else
            {
                return key[0] < id ? -1 : 1;
            }
        };
    }

    /**
     * Demands match on id and property val to be greater than prop
     * @param id    id to match
     * @param prop  property value to be greater than
     * @return      A new {@link RangePredicate} representing a greater than limit
     */
    public static RangePredicate greater( long id, long prop )
    {
        return key -> {
            if ( key[0] == id )
            {
                return key[1] > prop ? 0 : -1;
            }
            else
            {
                return key[0] < id ? -1 : 1;
            }
        };
    }

    /**
     * Demands match on id and property val to be greater than or equal to prop
     * @param id    id to match
     * @param prop  property value to be greater than or equal to
     * @return      A new {@link RangePredicate} representing a greater than or equal to limit
     */
    public static RangePredicate greaterOrEqual( long id, long prop )
    {
        return key -> {
            if ( key[0] == id )
            {
                return key[1] >= prop ? 0 : -1;
            }
            else
            {
                return key[0] < id ? -1 : 1;
            }
        };
    }

    /**
     * Demands match on id and property val
     * @param id    id to match
     * @param prop  property value to be equal to
     * @return      A new {@link RangePredicate} representing an equal to limit
     */
    public static RangePredicate equalTo( long id, long prop )
    {
        return key -> {
            int sign = Long.compare( key[0], id );
            return sign != 0 ? sign : Long.compare( key[1], prop );
        };
    }
}
