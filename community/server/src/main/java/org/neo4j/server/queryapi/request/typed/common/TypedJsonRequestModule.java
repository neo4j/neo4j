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
package org.neo4j.server.queryapi.request.typed.common;

import com.fasterxml.jackson.databind.module.SimpleModule;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.server.queryapi.request.QueryRequestCypherValues;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonBase64QueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonBooleanQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonDateQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonDurationQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonFloatQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonIntegerQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonListQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonLocalDateTimeQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonLocalTimeQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonMapQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonNullQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonOffsetDateTimeQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonPointQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonStringQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonTimeQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonUUIDQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonVectorQueryRequestCypherValue;
import org.neo4j.server.queryapi.request.typed.common.value.TypedJsonZonedDateTimeQueryRequestCypherValue;
import org.neo4j.server.queryapi.types.CypherTypes;
import org.neo4j.server.queryapi.types.View;

/**
 * The JSON module implementation for Typed JSON.
 * <p/>
 * This uses the {@link View} for defining which type are accepted or not by the module instantiation.
 */
public final class TypedJsonRequestModule extends SimpleModule {
    public TypedJsonRequestModule(View view) {
        this.addDeserializer(QueryRequestCypherValues.class, new TypedJsonQueryRequestCypherValuesDeserializer());

        var subtypes = new ArrayList<Class<?>>();

        putIfSupportedByView(subtypes, view, CypherTypes.Null, TypedJsonNullQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.String, TypedJsonStringQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.Boolean, TypedJsonBooleanQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.Integer, TypedJsonIntegerQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.Float, TypedJsonFloatQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.Base64, TypedJsonBase64QueryRequestCypherValue.class);
        putIfSupportedByView(
                subtypes, view, CypherTypes.OffsetDateTime, TypedJsonOffsetDateTimeQueryRequestCypherValue.class);
        putIfSupportedByView(
                subtypes, view, CypherTypes.ZonedDateTime, TypedJsonZonedDateTimeQueryRequestCypherValue.class);
        putIfSupportedByView(
                subtypes, view, CypherTypes.LocalDateTime, TypedJsonLocalDateTimeQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.Date, TypedJsonDateQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.Time, TypedJsonTimeQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.LocalTime, TypedJsonLocalTimeQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.Duration, TypedJsonDurationQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.Point, TypedJsonPointQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.UUID, TypedJsonUUIDQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.Map, TypedJsonMapQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.List, TypedJsonListQueryRequestCypherValue.class);
        putIfSupportedByView(subtypes, view, CypherTypes.Vector, TypedJsonVectorQueryRequestCypherValue.class);

        this.registerSubtypes(subtypes);
    }

    private static void putIfSupportedByView(
            List<Class<?>> subtypes,
            View view,
            CypherTypes cypherType,
            Class<? extends TypedJsonQueryRequestCypherValue> subtype) {
        if (view.supports(cypherType)) {
            subtypes.add(subtype);
        }
    }
}
