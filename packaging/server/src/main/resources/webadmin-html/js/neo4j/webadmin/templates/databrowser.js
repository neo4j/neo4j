/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
define(function(){return function(vars){ with(vars||{}) { return "<div class=\"span-2\"><div class=\"module\"><p class=\"info\">Browse the data in your neo4j server.</p><ul class=\"mor_horizontal_menu\"><li><button class=\"mor_data_add_node_button\">New node</button></li><li><button class=\"mor_data_add_relationship\">New relationship</button></li><li><button class=\"mor_data_refresh_button\">Refresh</button></li><li><button class=\"data_reference_node_button\">Go to reference node</button></li></ul><div class=\"break\"></div><div class=\"foldout\"><h2><a class=\"foldout_trigger\">More about the data browser</a></h2><div class=\"foldout_content\"><h2>Finding data</h2><p>You can enter the url of a node or a relationship below, to show it and its related nodes.</p><h2>Listing data</h2><p>Because each node can have any properties you want it to, the data browser allows you to select what properties it should list when listing related nodes.</p><p>Simply enter the properties you are interested in, separated by commas, in the \"properties to list\" box below.</p></div></div><div class=\"foldout\"><h2><a class=\"foldout_trigger\">More about relationships</a></h2><div class=\"foldout_content\"><h2>Getting the details</h2><p>When you are looking at a node, the \"related nodes\" table will only show direction and type of the various relationships connected to the node.</p><p>To view the properties of a relationship, click on the relationship type.</p><h2>Direction</h2><p>The arrows shown when listing related nodes show what direction the relationship goes.</p><p>If the arrow points to the <b>right</b>, this means it goes from the node you are focusing on, to the node that is shown in the table row.</p><p>If the arrow points to the <b>left</b>, this means it goes from the node that is shown in the table row , to the node you are focusing on.</p></div></div></div></div>";}}; });