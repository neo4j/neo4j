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
package org.neo4j.upgrade;

import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.storemigration.StoreUpgraderTest;

/**
 * Runs the store upgrader tests from older versions, migrating to the current enterprise version.
 */
public class StandardToEnterpriseStoreUpgraderTest extends StoreUpgraderTest
{
    public StandardToEnterpriseStoreUpgraderTest( RecordFormats recordFormats )
    {
        super( recordFormats );
    }

    @Override
    protected RecordFormats getRecordFormats()
    {
        return HighLimit.RECORD_FORMATS;
    }

    @Override
    protected String getRecordFormatsName()
    {
        return HighLimit.NAME;
    }
}
