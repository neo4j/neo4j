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
package org.neo4j.server.queryapi.request.common;

import static java.lang.String.format;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.io.IOException;
import java.util.HashMap;
import java.util.function.Function;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.server.queryapi.exception.ExceptionsUnwrapper;
import org.neo4j.server.queryapi.exception.QueryApiException;
import org.neo4j.server.queryapi.request.QueryRequestCypherValue;
import org.neo4j.server.queryapi.request.QueryRequestCypherValues;

/**
 * Common implementation of the {@link QueryRequestCypherValues} deserializer.
 * <p/>
 * This holds algorithm for reading keys and values from the JSON object independent of the
 * {@link org.neo4j.server.queryapi.QueryMimeTypes} used while allowing the specific type implementation
 * to be supplied on the constructor.
 *
 * @param <T> The sub implementation of {@link QueryRequestCypherValue} used in the context.
 */
public abstract class QueryRequestCypherValuesDeserializer<T extends QueryRequestCypherValue>
        extends StdDeserializer<QueryRequestCypherValues> {

    private final Function<Value, QueryRequestCypherValue> driverToQueryValue;
    private final Class<T> queryRequestCypherValueClass;

    protected QueryRequestCypherValuesDeserializer(
            Function<Value, QueryRequestCypherValue> driverToQueryValue, Class<T> queryRequestCypherValueClass) {
        super(TypeFactory.defaultInstance().constructType(QueryRequestCypherValues.class));

        this.driverToQueryValue = driverToQueryValue;
        this.queryRequestCypherValueClass = queryRequestCypherValueClass;
    }

    @Override
    public QueryRequestCypherValues deserialize(JsonParser p, DeserializationContext ctx) throws IOException {
        var map = new HashMap<String, QueryRequestCypherValue>();

        var t = p.currentToken();
        if (t != JsonToken.START_OBJECT && t != JsonToken.FIELD_NAME) {
            throw new JsonParseException("Unexpected token");
        }

        String keyString;
        if (p.isExpectedStartObjectToken()) {
            keyString = p.nextFieldName();
        } else {
            var token = p.currentToken();
            if (token == JsonToken.END_OBJECT) {
                return QueryRequestCypherValues.of(map);
            }
            if (token != JsonToken.FIELD_NAME) {
                ctx.reportWrongTokenException(this, JsonToken.FIELD_NAME, null);
            }
            keyString = p.currentName();
        }

        for (; keyString != null; keyString = p.nextFieldName()) {
            JsonToken nextToken = p.nextToken();
            try {
                if (nextToken == JsonToken.VALUE_NULL) {
                    map.put(keyString, driverToQueryValue.apply(Values.NULL));
                    continue;
                }
                var value = p.readValueAs(queryRequestCypherValueClass);
                map.put(keyString, value);
            } catch (QueryApiException e) {
                throw e;
            } catch (Exception e) {
                ExceptionsUnwrapper.unwrapAndThrowNeo4jAndQueryApiExceptions(e);
                throw new JsonParseException(format("Unable to read value for field %s", keyString));
            }
        }

        return QueryRequestCypherValues.of(map);
    }
}
