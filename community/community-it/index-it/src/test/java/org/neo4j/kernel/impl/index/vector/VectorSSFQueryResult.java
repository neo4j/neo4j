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
package org.neo4j.kernel.impl.index.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.assertj.core.api.Condition;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.TokenRead;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.internal.kernel.api.exceptions.PropertyKeyIdNotFoundKernelException;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class VectorSSFQueryResult {
    private final long entityId;
    private final float score;
    private final List<String> labels = new ArrayList<>();
    private final List<String> types = new ArrayList<>();
    private final Map<String, Value> values = new HashMap<>();

    public float score() {
        return score;
    }

    public long entityId() {
        return entityId;
    }

    @SuppressWarnings("unchecked")
    public <V extends Value> V getValue(String key) {
        return (V) values.get(key);
    }

    private VectorSSFQueryResult(long entityId, float score) {
        this.entityId = entityId;
        this.score = score;
    }

    private void addLabels(TokenSet labelTokens, TokenRead tokenRead) throws LabelNotFoundKernelException {
        for (int token : labelTokens.all()) {
            labels.add(tokenRead.nodeLabelName(token));
        }
    }

    private void addType(int typeToken, TokenRead tokenRead) throws KernelException {
        types.add(tokenRead.relationshipTypeName(typeToken));
    }

    /**
     * Add properties from the query to the result
     * Exclude the embedding vector
     */
    private void addProperties(PropertyCursor propertyCursor, TokenRead tokenRead, Set<String> exclude)
            throws PropertyKeyIdNotFoundKernelException {
        while (propertyCursor.next()) {
            String key = tokenRead.propertyKeyName(propertyCursor.propertyKey());
            if (!exclude.contains(key)) {
                Value value = propertyCursor.propertyValue();
                values.put(key, value);
            }
        }
    }

    static VectorSSFQueryResult fromCursors(
            float score, NodeCursor nodeCursor, PropertyCursor propertyCursor, TokenRead tokenRead, Set<String> exclude)
            throws LabelNotFoundKernelException, PropertyKeyIdNotFoundKernelException {
        VectorSSFQueryResult queryResult = new VectorSSFQueryResult(nodeCursor.nodeReference(), score);
        TokenSet labelTokens = nodeCursor.labelsAndProperties(propertyCursor, PropertySelection.ALL_PROPERTIES);
        queryResult.addLabels(labelTokens, tokenRead);
        queryResult.addProperties(propertyCursor, tokenRead, exclude);

        return queryResult;
    }

    static VectorSSFQueryResult fromCursors(
            float score,
            RelationshipScanCursor relCursor,
            PropertyCursor propertyCursor,
            TokenRead tokenRead,
            Set<String> exclude)
            throws KernelException {
        VectorSSFQueryResult queryResult = new VectorSSFQueryResult(relCursor.relationshipReference(), score);
        queryResult.addType(relCursor.type(), tokenRead);
        relCursor.properties(propertyCursor);
        queryResult.addProperties(propertyCursor, tokenRead, exclude);

        return queryResult;
    }

    public long getEntityId() {
        return entityId;
    }

    public Map<String, Value> mapForKeys(String... keys) {
        Map<String, Value> result = new HashMap<>(keys.length);
        for (String key : keys) {
            result.put(key, values.get(key));
        }
        return result;
    }

    public static Function<VectorSSFQueryResult, Object> extractor(String key) {
        return result -> result.values.get(key).asObject();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("score", score)
                .append("labels", labels)
                .append("values", values)
                .toString();
    }

    static Condition<VectorSSFQueryResult> field(String key, Object o) {
        Value value = Values.of(o);
        return new Condition<>(s -> s.getValue(key).equals(value), "value %s=%s", key, value);
    }

    static class ResultList extends ArrayList<VectorSSFQueryResult> {}
}
