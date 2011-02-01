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
 * Handles creating and removing relationships, changing relationship type and setting start and end nodes.
 */
wa.components.data.RelationshipManager = (function($) {
	
	var me = {};
	
	me.dataCore = wa.components.data.DataBrowser;
	
	me.web = new neo4j.Web();
	
	//
	// INTERNALS
	//
	
	me.server = function() {
		return me.dataCore.getServer();
	};
	
	me.addRelatiohship = function(ev) {
		ev.preventDefault();
		wa.ui.Dialog.showUsingTemplate("New relationship",
				"templates/components/data/new_relationship.tp", 
				{ fromNode : me.dataCore.getItem() },
				me.dialogLoaded);
	};
	
	me.saveNewRelationship = function(ev) {
		ev.preventDefault();
		var from = $("#mor_data_relationship_dialog_from").val();
		var type = $("#mor_data_relationship_dialog_type").val();
		var to = $("#mor_data_relationship_dialog_to").val();
		
		if( from.indexOf("://") ) {
			from = from.substring(from.lastIndexOf("/")+1);
		}
		
		if( ! to.indexOf("://")) {
			to = me.server().url + to;
		}
		
		if ( to === from ) {
			wa.ui.ErrorBox.showError("You cannot create self-relationships.");
		} else {
			me.server().post("node/" + from + "/relationships", {
					"to" : to,
					"data" : {},
					"type": type
				}, function(data) {
				    wa.components.data.DataBrowser.reload();
					wa.ui.Dialog.close();
				}
			);
		}
	};
	
	/**
	 * This is called each time the create relationship dialog is shown.
	 */
	me.dialogLoaded = function() {
		// Populate from field
		var id = me.dataCore.getItem().self;
		id = id.substring(id.lastIndexOf("/") + 1);
		
		$("#mor_data_relationship_dialog_from").val(id);
	};
	
	me.deleteItem = function(ev) {
		ev.preventDefault();
		if( confirm("Are you sure?")) {
		    me.web.del(me.dataCore.getItem().self, function(data) {
				// Go to root node
				$.bbq.pushState({ dataurl: "node/0" });
			});
		}
	};
	
	$(".mor_data_delete_relationship_button").live("click", me.deleteItem);
	$(".mor_data_add_relationship").live("click", me.addRelatiohship);
	$(".mor_data_relationship_dialog_save").live("click", me.saveNewRelationship);
	
	return {};
	
})(jQuery);