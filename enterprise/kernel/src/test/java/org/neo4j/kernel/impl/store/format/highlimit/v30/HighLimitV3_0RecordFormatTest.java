/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.store.format.highlimit.v30;

import org.neo4j.kernel.impl.store.format.AbstractRecordFormatTest;
import org.neo4j.kernel.impl.store.format.highlimit.v300.HighLimitV3_0_0;

public class HighLimitV3_0RecordFormatTest extends AbstractRecordFormatTest
{
    public HighLimitV3_0RecordFormatTest()
    {
        super( HighLimitV3_0_0.RECORD_FORMATS, 50, 50 );
    }
}
