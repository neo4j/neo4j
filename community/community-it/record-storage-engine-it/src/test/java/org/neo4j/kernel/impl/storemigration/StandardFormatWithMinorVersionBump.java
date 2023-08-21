/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.storemigration;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV5_0;

public class StandardFormatWithMinorVersionBump extends StandardV5_0 {
    public static final RecordFormats RECORD_FORMATS = new StandardFormatWithMinorVersionBump();
    public static final String NAME = "Standard-Format-With-Minor-Version-Bump";

    private StandardFormatWithMinorVersionBump() {}

    @Override
    public int minorVersion() {
        return Standard.LATEST_RECORD_FORMATS.minorVersion() + 1;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean formatUnderDevelopment() {
        return true;
    }

    @ServiceProvider
    public static class Factory implements RecordFormats.Factory {
        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public RecordFormats getInstance() {
            return RECORD_FORMATS;
        }
    }
}
