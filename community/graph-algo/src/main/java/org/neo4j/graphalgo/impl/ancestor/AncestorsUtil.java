/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.graphalgo.impl.ancestor;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;

/**
 * @author Pablo Pareja Tobes
 */
public class AncestorsUtil {

    /**
     * 
     * @param nodeSet Set of nodes for which the LCA will be found.
     * @return The LCA node if there's one, null otherwise.
     */
    public static Node lowestCommonAncestor(List<Node> nodeSet,
            RelationshipExpander expander) {

        Node lowerCommonAncestor = null;

        if (nodeSet.size() > 1) {

            Node firstNode = nodeSet.get(0);
            LinkedList<Node> firstAncestors = getAncestorsPlusSelf(firstNode, expander);

            for (int i = 1; i < nodeSet.size() && !firstAncestors.isEmpty(); i++) {
                Node currentNode = nodeSet.get(i);
                lookForCommonAncestor(firstAncestors, currentNode, expander);                
            }
            
            if(!firstAncestors.isEmpty()){
                lowerCommonAncestor = firstAncestors.get(0);
            }
            
        }

        return lowerCommonAncestor;
    }

    private static LinkedList<Node> getAncestorsPlusSelf(Node node,
            RelationshipExpander expander) {
        
        LinkedList<Node> ancestors = new LinkedList<Node>();

        ancestors.add(node);
        Iterator<Relationship> relIterator = expander.expand(node).iterator();

        while (relIterator.hasNext()) {

            Relationship rel = relIterator.next();
            node = rel.getOtherNode(node);       

            ancestors.add(node);

            relIterator = expander.expand(node).iterator();

        }

        return ancestors;

    }

    private static void lookForCommonAncestor(LinkedList<Node> commonAncestors,
            Node currentNode,
            RelationshipExpander expander) {

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

            Iterator<Relationship> relIt = expander.expand(currentNode).iterator();

            if (relIt.hasNext()) {
                
                Relationship rel = relIt.next();
                
                currentNode = rel.getOtherNode(currentNode); 
                
            }else{
                currentNode = null;
            }
        }

    }
}
