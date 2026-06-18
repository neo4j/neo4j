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
package org.neo4j.kernel.impl.query.statistic;

import java.util.LinkedHashMap;
import java.util.Map;

public class PlanOperatorDetailsToBeLogged {
    private String operatorName;
    private int operatorId;
    private int leftChildOperatorId;
    private int rightChildOperatorId;
    private String details;
    private Map<String, Double> estimatedRows;
    private String order;
    private String distinctness;
    private Map<String, Object> pipelineInfo;

    public PlanOperatorDetailsToBeLogged() {
        this.operatorName = null;
        this.operatorId = -1;
        this.leftChildOperatorId = -1;
        this.rightChildOperatorId = -1;
        this.details = null;
        this.estimatedRows = null;
        this.order = null;
        this.distinctness = null;
        this.pipelineInfo = null;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public void setOperatorId(int operatorId) {
        this.operatorId = operatorId;
    }

    public void setLeftOperatorId(int leftOperatorId) {
        this.leftChildOperatorId = leftOperatorId;
    }

    public void setRightOperatorId(int rightOperatorId) {
        this.rightChildOperatorId = rightOperatorId;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public void setEstimatedRows(Map<String, Double> estimatedRows) {
        this.estimatedRows = estimatedRows;
    }

    public void setPipelineInfo(Map<String, Object> pipelineInfo) {
        this.pipelineInfo = pipelineInfo;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public void setDistinctness(String distinctness) {
        this.distinctness = distinctness;
    }

    public Map<String, Object> toMap() {
        var map = new LinkedHashMap<String, Object>();
        if (operatorName != null) map.put("operatorName", operatorName);
        if (operatorId != -1) map.put("operatorId", operatorId);
        if (leftChildOperatorId != -1) map.put("leftChildOperatorId", leftChildOperatorId);
        if (rightChildOperatorId != -1) map.put("rightChildOperatorId", rightChildOperatorId);
        if (details != null) map.put("details", details);
        if (estimatedRows != null) map.put("estimatedRows", estimatedRows);
        if (order != null) map.put("order", order);
        if (distinctness != null) map.put("distinctness", distinctness);
        if (pipelineInfo != null) map.put("pipelineInfo", pipelineInfo);
        return map;
    }
}
