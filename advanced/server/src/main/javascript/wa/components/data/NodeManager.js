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

/**
 * Handles creating and removing nodes.
 */
wa.components.data.NodeManager = (function($) { 
	
	var me = {};
	
	me.item = wa.components.data.DataBrowser.getItem;
	me.showReferenceNode = wa.components.data.DataBrowser.showReferenceNode;
    me.server = wa.components.data.DataBrowser.getServer;
	
	//
	// INTERNALS
	//
	
	me.addNode = function(ev) {
		ev.preventDefault();
		me.server().node({}).then(function(node) {
			$.bbq.pushState({ dataurl: node.getSelf() });
		});
	};
	
	me.deleteItem = function(ev) {
		ev.preventDefault();
		if( confirm("Are you sure?")) {
		    me.item().remove().then(function(data) {
		        me.showReferenceNode();
			});
		}
	};
	
	$(".mor_data_add_node_button").live("click", me.addNode);
	$(".mor_data_delete_node_button").live("click", me.deleteItem);
	
	return {};
	
})(jQuery);