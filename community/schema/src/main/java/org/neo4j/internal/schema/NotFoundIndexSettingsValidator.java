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
package org.neo4j.internal.schema;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Supplier;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.graphdb.schema.IndexSetting;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;

public class NotFoundIndexSettingsValidator implements IndexSettingsValidator {
    protected final Supplier<InvalidArgumentException> exception;

    public NotFoundIndexSettingsValidator(Supplier<InvalidArgumentException> exception) {
        this.exception = exception;
    }

    @Override
    public IndexSettingRecordsByState validate(SettingsAccessor accessor) {
        throw exception.get();
    }

    @Override
    public SortedSet<Valid> interpretAuthoritative(SettingsAccessor accessor) {
        throw exception.get();
    }

    @Override
    public Set<IndexSetting> acceptedSettings() {
        return Collections.emptySet();
    }
}
