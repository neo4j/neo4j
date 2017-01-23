/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
// Generated from /Users/mats/gitRoots/3.0-copy/neo4j/community/cypher/compatibility-suite/src/main/resources/FeatureResults.g4 by ANTLR 4.5.1
package cypher.feature.parser.generated;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link FeatureResultsParser}.
 */
public interface FeatureResultsListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#value}.
	 * @param ctx the parse tree
	 */
	void enterValue(FeatureResultsParser.ValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#value}.
	 * @param ctx the parse tree
	 */
	void exitValue(FeatureResultsParser.ValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#node}.
	 * @param ctx the parse tree
	 */
	void enterNode(FeatureResultsParser.NodeContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#node}.
	 * @param ctx the parse tree
	 */
	void exitNode(FeatureResultsParser.NodeContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#nodeDesc}.
	 * @param ctx the parse tree
	 */
	void enterNodeDesc(FeatureResultsParser.NodeDescContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#nodeDesc}.
	 * @param ctx the parse tree
	 */
	void exitNodeDesc(FeatureResultsParser.NodeDescContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#relationship}.
	 * @param ctx the parse tree
	 */
	void enterRelationship(FeatureResultsParser.RelationshipContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#relationship}.
	 * @param ctx the parse tree
	 */
	void exitRelationship(FeatureResultsParser.RelationshipContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#relationshipDesc}.
	 * @param ctx the parse tree
	 */
	void enterRelationshipDesc(FeatureResultsParser.RelationshipDescContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#relationshipDesc}.
	 * @param ctx the parse tree
	 */
	void exitRelationshipDesc(FeatureResultsParser.RelationshipDescContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#path}.
	 * @param ctx the parse tree
	 */
	void enterPath(FeatureResultsParser.PathContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#path}.
	 * @param ctx the parse tree
	 */
	void exitPath(FeatureResultsParser.PathContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#pathBody}.
	 * @param ctx the parse tree
	 */
	void enterPathBody(FeatureResultsParser.PathBodyContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#pathBody}.
	 * @param ctx the parse tree
	 */
	void exitPathBody(FeatureResultsParser.PathBodyContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#pathLink}.
	 * @param ctx the parse tree
	 */
	void enterPathLink(FeatureResultsParser.PathLinkContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#pathLink}.
	 * @param ctx the parse tree
	 */
	void exitPathLink(FeatureResultsParser.PathLinkContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#forwardsRelationship}.
	 * @param ctx the parse tree
	 */
	void enterForwardsRelationship(FeatureResultsParser.ForwardsRelationshipContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#forwardsRelationship}.
	 * @param ctx the parse tree
	 */
	void exitForwardsRelationship(FeatureResultsParser.ForwardsRelationshipContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#backwardsRelationship}.
	 * @param ctx the parse tree
	 */
	void enterBackwardsRelationship(FeatureResultsParser.BackwardsRelationshipContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#backwardsRelationship}.
	 * @param ctx the parse tree
	 */
	void exitBackwardsRelationship(FeatureResultsParser.BackwardsRelationshipContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#integer}.
	 * @param ctx the parse tree
	 */
	void enterInteger(FeatureResultsParser.IntegerContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#integer}.
	 * @param ctx the parse tree
	 */
	void exitInteger(FeatureResultsParser.IntegerContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#floatingPoint}.
	 * @param ctx the parse tree
	 */
	void enterFloatingPoint(FeatureResultsParser.FloatingPointContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#floatingPoint}.
	 * @param ctx the parse tree
	 */
	void exitFloatingPoint(FeatureResultsParser.FloatingPointContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#bool}.
	 * @param ctx the parse tree
	 */
	void enterBool(FeatureResultsParser.BoolContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#bool}.
	 * @param ctx the parse tree
	 */
	void exitBool(FeatureResultsParser.BoolContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#nullValue}.
	 * @param ctx the parse tree
	 */
	void enterNullValue(FeatureResultsParser.NullValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#nullValue}.
	 * @param ctx the parse tree
	 */
	void exitNullValue(FeatureResultsParser.NullValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#list}.
	 * @param ctx the parse tree
	 */
	void enterList(FeatureResultsParser.ListContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#list}.
	 * @param ctx the parse tree
	 */
	void exitList(FeatureResultsParser.ListContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#listContents}.
	 * @param ctx the parse tree
	 */
	void enterListContents(FeatureResultsParser.ListContentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#listContents}.
	 * @param ctx the parse tree
	 */
	void exitListContents(FeatureResultsParser.ListContentsContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#listElement}.
	 * @param ctx the parse tree
	 */
	void enterListElement(FeatureResultsParser.ListElementContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#listElement}.
	 * @param ctx the parse tree
	 */
	void exitListElement(FeatureResultsParser.ListElementContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#map}.
	 * @param ctx the parse tree
	 */
	void enterMap(FeatureResultsParser.MapContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#map}.
	 * @param ctx the parse tree
	 */
	void exitMap(FeatureResultsParser.MapContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#propertyMap}.
	 * @param ctx the parse tree
	 */
	void enterPropertyMap(FeatureResultsParser.PropertyMapContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#propertyMap}.
	 * @param ctx the parse tree
	 */
	void exitPropertyMap(FeatureResultsParser.PropertyMapContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#mapContents}.
	 * @param ctx the parse tree
	 */
	void enterMapContents(FeatureResultsParser.MapContentsContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#mapContents}.
	 * @param ctx the parse tree
	 */
	void exitMapContents(FeatureResultsParser.MapContentsContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#keyValuePair}.
	 * @param ctx the parse tree
	 */
	void enterKeyValuePair(FeatureResultsParser.KeyValuePairContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#keyValuePair}.
	 * @param ctx the parse tree
	 */
	void exitKeyValuePair(FeatureResultsParser.KeyValuePairContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#propertyKey}.
	 * @param ctx the parse tree
	 */
	void enterPropertyKey(FeatureResultsParser.PropertyKeyContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#propertyKey}.
	 * @param ctx the parse tree
	 */
	void exitPropertyKey(FeatureResultsParser.PropertyKeyContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void enterPropertyValue(FeatureResultsParser.PropertyValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#propertyValue}.
	 * @param ctx the parse tree
	 */
	void exitPropertyValue(FeatureResultsParser.PropertyValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#relationshipType}.
	 * @param ctx the parse tree
	 */
	void enterRelationshipType(FeatureResultsParser.RelationshipTypeContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#relationshipType}.
	 * @param ctx the parse tree
	 */
	void exitRelationshipType(FeatureResultsParser.RelationshipTypeContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#relationshipTypeName}.
	 * @param ctx the parse tree
	 */
	void enterRelationshipTypeName(FeatureResultsParser.RelationshipTypeNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#relationshipTypeName}.
	 * @param ctx the parse tree
	 */
	void exitRelationshipTypeName(FeatureResultsParser.RelationshipTypeNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#label}.
	 * @param ctx the parse tree
	 */
	void enterLabel(FeatureResultsParser.LabelContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#label}.
	 * @param ctx the parse tree
	 */
	void exitLabel(FeatureResultsParser.LabelContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#labelName}.
	 * @param ctx the parse tree
	 */
	void enterLabelName(FeatureResultsParser.LabelNameContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#labelName}.
	 * @param ctx the parse tree
	 */
	void exitLabelName(FeatureResultsParser.LabelNameContext ctx);
	/**
	 * Enter a parse tree produced by {@link FeatureResultsParser#string}.
	 * @param ctx the parse tree
	 */
	void enterString(FeatureResultsParser.StringContext ctx);
	/**
	 * Exit a parse tree produced by {@link FeatureResultsParser#string}.
	 * @param ctx the parse tree
	 */
	void exitString(FeatureResultsParser.StringContext ctx);
}