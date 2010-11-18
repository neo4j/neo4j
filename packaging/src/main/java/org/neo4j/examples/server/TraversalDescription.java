package org.neo4j.examples.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TraversalDescription {

    public static final String DEPTH_FIRST = "depth first";
    public static final String NODE = "node";
    public static final String ALL = "all";

    private String uniqueness = NODE;
    private int maxDepth = 1;
    private String returnFilter = ALL;
    private String order = DEPTH_FIRST;
    private List<Relationship> relationships = new ArrayList<Relationship>();

    public void setOrder(String order) {
        this.order = order;
    }

    public void setUniqueness(String uniqueness) {
        this.uniqueness = uniqueness;
    }

    public void setMaxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
    }

    public void setReturnFilter(String returnFilter) {
        this.returnFilter = returnFilter;
    }

    public void setRelationships(Relationship... relationships) {
        this.relationships =  Arrays.asList(relationships);
    }

    public String toJson() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ ");
        sb.append(" \"order\" : \"" + order + "\"");
        sb.append(", ");
        sb.append(" \"uniqueness\" : \"" + uniqueness + "\"");
        sb.append(", ");
        if (relationships.size() > 0) {
            sb.append("\"relationships\" : [");
            for (int i = 0; i < relationships.size(); i++) {
                sb.append(relationships.get(i).toJsonCollection());
                if (i < relationships.size() - 1) { // Miss off the final comma
                    sb.append(", ");
                }
            }
            sb.append("], ");
        }
        sb.append("\"return filter\" : { ");
        sb.append("\"language\" : \"builtin\", ");
        sb.append("\"name\" : \"");
        sb.append(returnFilter);
        sb.append("\" }, ");
        sb.append("\"max depth\" : ");
        sb.append(maxDepth);
        sb.append(" }");
        return sb.toString();
    }
}
