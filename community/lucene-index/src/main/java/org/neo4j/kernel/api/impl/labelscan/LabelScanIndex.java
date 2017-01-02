/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.impl.labelscan;

import org.neo4j.kernel.api.impl.index.DatabaseIndex;
import org.neo4j.kernel.api.labelscan.AllEntriesLabelScanReader;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelRange;
import org.neo4j.storageengine.api.schema.LabelScanReader;

/**
 * Partitioned lucene labels Label scan index.
 */
public interface LabelScanIndex extends DatabaseIndex
{
    LabelScanReader getLabelScanReader();

    LabelScanWriter getLabelScanWriter();

    /**
     * Retrieves a {@link AllEntriesLabelScanReader reader} over all {@link NodeLabelRange node label} ranges.
     * <p>
     * <b>NOTE:</b>
     * There are no guarantees that reader returned from this method will see consistent documents with respect to
     * {@link #getLabelScanReader() regular reader} and {@link #getLabelScanWriter() regular writer}.
     *
     * @return the {@link AllEntriesLabelScanReader reader}.
     */
    AllEntriesLabelScanReader allNodeLabelRanges();
}
