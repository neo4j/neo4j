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
package org.neo4j.kernel.impl.store.format.highlimit;

import org.neo4j.kernel.impl.store.format.FormatFamily;

/**
 * High limit format family.
 * @see FormatFamily
 */
public class HighLimitFormatFamily extends FormatFamily
{
    public static final FormatFamily INSTANCE = new HighLimitFormatFamily();

    private static final String HIGH_LIMIT_FORMAT_FAMILY_NAME = "High limit format family";

    private HighLimitFormatFamily()
    {
    }

    @Override
    public String getName()
    {
        return HIGH_LIMIT_FORMAT_FAMILY_NAME;
    }

    @Override
    public int rank()
    {
        return 1;
    }

}
