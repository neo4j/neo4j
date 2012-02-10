/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.neo4j.graphalgo.impl.ancestor;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * @author Pablo Pareja Tobes
 */
public class AncestorsUtil {

    /**
     * 
     * @param nodeSet Set of nodes for which the LCA will be found.
     * @param relationshipType Relationship type used to look for the LCA
     * @param relationshipDirection Direction of the relationships used (seen from the descendant node)
     * @return The LCA node if there's one, null otherwise.
     */
    public static Node lowestCommonAncestor(List<Node> nodeSet,
            RelationshipType relationshipType,
            Direction relationshipDirection) {

        Node lowerCommonAncestor = null;

        if (nodeSet.size() > 1) {

            Node firstNode = nodeSet.get(0);
            LinkedList<Node> firstAncestors = getAncestorsPlusSelf(firstNode, relationshipType, relationshipDirection);

            for (int i = 1; i < nodeSet.size() && !firstAncestors.isEmpty(); i++) {
                Node currentNode = nodeSet.get(i);
                lookForCommonAncestor(firstAncestors, currentNode, relationshipType, relationshipDirection);                
            }
            
            if(!firstAncestors.isEmpty()){
                lowerCommonAncestor = firstAncestors.get(0);
            }
            
        }

        return lowerCommonAncestor;
    }

    private static LinkedList<Node> getAncestorsPlusSelf(Node node,
            RelationshipType relationship,
            Direction direction) {

        LinkedList<Node> ancestors = new LinkedList<Node>();

        ancestors.add(node);
        Iterator<Relationship> relIterator = node.getRelationships(relationship, direction).iterator();

        while (relIterator.hasNext()) {

            Relationship rel = relIterator.next();
            Node parentNode = null;
            if (direction.equals(Direction.INCOMING)) {
                parentNode = rel.getStartNode();
            } else {
                parentNode = rel.getEndNode();
            }

            ancestors.add(parentNode);

            relIterator = parentNode.getRelationships(relationship, direction).iterator();

        }

        return ancestors;

    }

    private static void lookForCommonAncestor(LinkedList<Node> commonAncestors,
            Node currentNode,
            RelationshipType relationship,
            Direction direction) {

        while (currentNode != null) {

            for (int i = 0; i < commonAncestors.size(); i++) {
                Node node = commonAncestors.get(i);
                if (node.getId() == currentNode.getId()) {
                    for (int j = 0; j < i; j++) {
                        commonAncestors.pollFirst();
                    }
                    return;
                }
            }

            Iterator<Relationship> relIt = currentNode.getRelationships(relationship, direction).iterator();

            if (relIt.hasNext()) {
                
                Relationship rel = relIt.next();
                
                if (direction.equals(Direction.INCOMING)) {
                    currentNode = rel.getStartNode();
                } else {
                    currentNode = rel.getEndNode();
                }
                
            }else{
                currentNode = null;
            }
        }

    }
}
