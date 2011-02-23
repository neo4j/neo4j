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
		me.server().getAvailableRelationshipTypes().then(function(types) {
    		wa.ui.Dialog.showUsingTemplate("New relationship",
    				"templates/components/data/new_relationship.tp", 
    				{ fromNode : me.dataCore.getItem(), types:types },
    				me.dialogLoaded);
		});
	};
	
	me.saveNewRelationship = function(ev) {
		ev.preventDefault();
		var from = $("#mor_data_relationship_dialog_from").val();
		var type = $("#mor_data_relationship_dialog_type").val();
		var to = $("#mor_data_relationship_dialog_to").val();
		
		if ( to === from ) {
			wa.ui.ErrorBox.showError("You cannot create self-relationships.");
		} else if (type.length === 0) {
		    wa.ui.ErrorBox.showError("You have to enter a relationship type. Any name will be fine, like 'KNOWS'.");
		} else {
			me.server().rel(from, type, to).then(function(){
			    wa.components.data.DataBrowser.reload();
				wa.ui.Dialog.close();
			}, function(err) {
			    if(err instanceof neo4j.exceptions.NotFoundException) {
			        wa.ui.ErrorBox.showError("'" + err.url + "' could not be found, unable to create relationship.");
			    } else {
			        wa.ui.ErrorBox.showError("An unknown error occurred, unable to create relationship.");
			    }
			});
			
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
	
	me.availableTypeDropdownChanged = function(ev) {
	    
	    var pickedVal = $(ev.target).val(),
	        opts = $("#mor_data_relationship_available_types .selectable");

        // Ensure the item we picked has the "selectable" class.
	    var pickedItem = _.detect(opts, function(opt) { return $(opt).html() == pickedVal; });
	    if(typeof(pickedItem) != "undefined") {
	        $("#mor_data_relationship_dialog_type").val(pickedVal);
	    }

	    // Reset the dropdown
        $(ev.target).val($($(ev.target).children()[0]).html());
	    
	};
	
	$(".mor_data_delete_relationship_button").live("click", me.deleteItem);
	$(".mor_data_add_relationship").live("click", me.addRelatiohship);
	$(".mor_data_relationship_dialog_save").live("click", me.saveNewRelationship);
	$("#mor_data_relationship_available_types").live("change", me.availableTypeDropdownChanged);
	
	$("#mor_data_relationship_dialog_to").live("keydown", function(e) {
	    if(e.keyCode == 13) {
	        me.saveNewRelationship(e);
	    }
	});
	
	return {};
	
})(jQuery);