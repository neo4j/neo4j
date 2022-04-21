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

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.format.standard.StandardV5_0;

@ServiceProvider
public class StandardFormatWithMinorVersionBump extends StandardV5_0 implements RecordFormats.Factory {
    public static final String NAME = "Standard-Format-With-Minor-Version-Bump";
    public static final String VERSION_STRING = "TSF1.2";

    public StandardFormatWithMinorVersionBump() {
        super();
    }

    @Override
    public int minorVersion() {
        return Standard.LATEST_RECORD_FORMATS.minorVersion() + 1;
    }

    @Override
    public String storeVersion() {
        return VERSION_STRING;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public boolean formatUnderDevelopment() {
        return true;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public RecordFormats newInstance() {
        return new StandardFormatWithMinorVersionBump();
    }
}
