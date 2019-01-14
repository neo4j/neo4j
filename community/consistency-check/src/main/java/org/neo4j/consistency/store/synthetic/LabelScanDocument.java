/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.consistency.store.synthetic;

import org.neo4j.kernel.api.labelscan.NodeLabelRange;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

/**
 * Synthetic record type that stands in for a real record to fit in conveniently
 * with consistency checking
 */
public class LabelScanDocument extends AbstractBaseRecord
{
    private NodeLabelRange nodeLabelRange;

    public LabelScanDocument( NodeLabelRange nodeLabelRange )
    {
        super( nodeLabelRange.id() );
        this.nodeLabelRange = nodeLabelRange;
        setInUse( true );
    }

    @Override
    public void clear()
    {
        super.clear();
        this.nodeLabelRange = null;
    }

    public NodeLabelRange getNodeLabelRange()
    {
        return nodeLabelRange;
    }

    @Override
    public String toString()
    {
        return nodeLabelRange.toString();
    }
}
