/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.storemigration;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.aligned.PageAlignedV4_1;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_4;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_0;
import org.neo4j.kernel.impl.store.format.standard.StandardV4_3;
import org.neo4j.storageengine.migration.RollingUpgradeCompatibility;

import static org.assertj.core.api.Assertions.assertThat;

class RecordStoreRollingUpgradeCompatibilityTest
{
    private final RecordStorageEngineFactory storageEngineFactory = new RecordStorageEngineFactory();
    private final RollingUpgradeCompatibility rollingUpgradeCompatibility = storageEngineFactory.rollingUpgradeCompatibility();

    @Test
    void shouldFindMinorUpgradableFormatsCompatible()
    {
        assertThat( isStoreFormatsCompatibleIncludingMinorUpgradable( StandardV4_0.RECORD_FORMATS, StandardV4_3.RECORD_FORMATS ) ).isTrue();
        assertThat( isStoreFormatsCompatibleIncludingMinorUpgradable( StandardV4_3.RECORD_FORMATS, StandardV4_3.RECORD_FORMATS ) ).isTrue();
        assertThat( isStoreFormatsCompatibleIncludingMinorUpgradable( StandardV4_3.RECORD_FORMATS, StandardV4_0.RECORD_FORMATS ) ).isFalse();
        assertThat( isStoreFormatsCompatibleIncludingMinorUpgradable( StandardV3_4.RECORD_FORMATS, StandardV4_3.RECORD_FORMATS ) ).isFalse();
        assertThat( isStoreFormatsCompatibleIncludingMinorUpgradable( PageAlignedV4_1.RECORD_FORMATS, StandardV4_3.RECORD_FORMATS ) ).isFalse();
        assertThat( isStoreFormatsCompatibleIncludingMinorUpgradable( PageAlignedV4_1.RECORD_FORMATS, StandardV4_3.RECORD_FORMATS ) ).isFalse();
    }

    @Test
    void shouldNotThrowOnUnknownStoreVersion()
    {
        assertThat( rollingUpgradeCompatibility.isVersionCompatibleForRollingUpgrade( StandardV4_0.RECORD_FORMATS.storeVersion(), "foo" ) ).isFalse();
        assertThat( rollingUpgradeCompatibility.isVersionCompatibleForRollingUpgrade( "foo", StandardV4_0.RECORD_FORMATS.storeVersion() ) ).isFalse();
    }

    private boolean isStoreFormatsCompatibleIncludingMinorUpgradable( RecordFormats format, RecordFormats otherFormat )
    {
        return rollingUpgradeCompatibility.isVersionCompatibleForRollingUpgrade( format.storeVersion(), otherFormat.storeVersion() );
    }
}
