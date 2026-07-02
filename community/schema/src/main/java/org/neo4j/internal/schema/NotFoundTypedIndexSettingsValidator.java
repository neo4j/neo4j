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

import java.util.function.Supplier;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.internal.schema.IndexSettingRecord.Valid;

public class NotFoundTypedIndexSettingsValidator<CONFIG extends TypedIndexConfig>
        extends TypedIndexSettingsValidator<CONFIG> {
    protected final Supplier<InvalidArgumentException> exception;

    public NotFoundTypedIndexSettingsValidator(
            IndexProviderDescriptor descriptor, Supplier<InvalidArgumentException> exception) {
        super(descriptor, new NotFoundIndexSettingsValidator(exception));
        this.exception = exception;
    }

    @Override
    public CONFIG validateToTypedConfig(SettingsAccessor accessor) {
        throw exception.get();
    }

    @Override
    public CONFIG validateToTypedConfig(IndexSettingRecordsByState records) {
        throw exception.get();
    }

    @Override
    public CONFIG interpretAuthoritativeToTypedConfig(SettingsAccessor accessor) {
        throw exception.get();
    }

    @Override
    public CONFIG interpretAuthoritativeToTypedConfig(Iterable<Valid> records) {
        throw exception.get();
    }

    @Override
    protected CONFIG toTypedConfig(Iterable<Valid> records) {
        throw exception.get();
    }
}
