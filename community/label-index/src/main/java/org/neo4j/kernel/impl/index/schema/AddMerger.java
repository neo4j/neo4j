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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.ValueMerger;

class AddMerger implements ValueMerger<TokenScanKey,TokenScanValue>
{
    private final NativeTokenScanWriter.WriteMonitor monitor;

    AddMerger( NativeTokenScanWriter.WriteMonitor monitor )
    {
        this.monitor = monitor;
    }

    @Override
    public MergeResult merge( TokenScanKey existingKey, TokenScanKey newKey, TokenScanValue existingValue, TokenScanValue newValue )
    {
        monitor.mergeAdd( existingValue, newValue );
        existingValue.add( newValue );
        return MergeResult.MERGED;
    }
}
