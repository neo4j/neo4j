package org.neo4j.index.btree.util;

import org.neo4j.index.Seeker;
import org.neo4j.index.btree.RangePredicate;
import org.neo4j.index.btree.RangeSeeker;
import org.neo4j.index.btree.Scanner;

public class SeekerFactory
{
    public static Seeker scanner()
    {
        return new Scanner();
    }

    public static Seeker exactMatch( long id, long prop )
    {
        return new RangeSeeker( RangePredicate.equalTo( id, prop ), RangePredicate.equalTo( id, prop ) );
    }
}
