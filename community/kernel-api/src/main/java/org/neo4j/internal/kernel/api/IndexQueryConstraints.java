/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api;

import java.util.OptionalLong;

import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.util.Preconditions;

public class IndexQueryConstraints
{
    private static final IndexQueryConstraints UNCONSTRAINED = new IndexQueryConstraints( IndexOrder.NONE, false, false, false, 0, 0 );
    private static final IndexQueryConstraints UNORDERED_VALUES = new IndexQueryConstraints( IndexOrder.NONE, true, false, false, 0, 0 );

    private final IndexOrder order;
    private final boolean needsValues;
    private final boolean hasSkip;
    private final boolean hasLimit;
    private final long skip;
    private final long limit;

    private IndexQueryConstraints( IndexOrder order, boolean needsValues, boolean hasSkip, boolean hasLimit, long skip, long limit )
    {
        this.order = order;
        this.needsValues = needsValues;
        this.hasSkip = hasSkip;
        this.hasLimit = hasLimit;
        this.skip = skip;
        this.limit = limit;
    }

    public static IndexQueryConstraints unconstrained()
    {
        return UNCONSTRAINED;
    }

    public static IndexQueryConstraints unorderedValues()
    {
        return UNORDERED_VALUES;
    }

    public static IndexQueryConstraints unordered( boolean needsValues )
    {
        return needsValues ? unorderedValues() : unconstrained();
    }

    public static IndexQueryConstraints constrained( IndexOrder order, boolean needsValues )
    {
        return new IndexQueryConstraints( order, needsValues, false, false, 0, 0 );
    }

    public IndexQueryConstraints skip( long skip )
    {
        Preconditions.checkState( skip >= 0, "Skip argument cannot be negative: %s.", skip );
        if ( hasLimit )
        {
            Preconditions.requireNoLongAddOverflow( skip, limit, "SKIP (%s) and LIMIT (%s) combined are too large; would overflow 64-bit signed integer." );
        }
        return new IndexQueryConstraints( order, needsValues, true, hasLimit, skip, limit );
    }

    public IndexQueryConstraints limit( long limit )
    {
        Preconditions.checkState( limit >= 0, "Limit argument cannot be negative: %s.", limit );
        if ( hasSkip )
        {
            Preconditions.requireNoLongAddOverflow( skip, limit, "SKIP (%s) and LIMIT (%s) are too large; would overflow 64-bit signed integer." );
        }
        return new IndexQueryConstraints( order, needsValues, hasSkip, true, skip, limit );
    }

    public boolean isOrdered()
    {
        return order != IndexOrder.NONE;
    }

    public boolean needsValues()
    {
        return needsValues;
    }

    public IndexOrder order()
    {
        return order;
    }

    public OptionalLong skip()
    {
        return hasSkip ? OptionalLong.of( skip ) : OptionalLong.empty();
    }

    public OptionalLong limit()
    {
        return hasLimit ? OptionalLong.of( limit ) : OptionalLong.empty();
    }
}
