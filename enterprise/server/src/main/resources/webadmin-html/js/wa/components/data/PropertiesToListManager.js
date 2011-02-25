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
 * Handles the control for what properties to list in the related nodes table in the
 * data browser.
 */
wa.components.data.PropertiesToListManager = (function($) { 
	
	var me = {};
	
	me.listFields = ['name'];
	me.availableFields = [];
	
	//
	// INTERNALS
	//
	
	// 
	// CONSTRUCT
	//
	
	$("#mor_data_listfields_button").live("click", function(ev) {
		ev.preventDefault();
        me.listFields = [];
        
		var fieldString = $("#mor_data_listfields").val();
        var fields = fieldString.split(",");
        
        for(var i=0,l=fields.length; i<l; i++) {
            me.listFields.push($.trim(fields[i]));
        }
        
        wa.trigger("data.listnames.changed");
	});
	
	return {
        getListFields : function() {
            return me.listFields;
        },
        
        /**
         * Given a list of nodes, pick some common field names to list.
         */
        setFromNodeList : function(nodes) {
            var foundFields = {};
            _.each(nodes, function(node){
               _.each(_.keys(node.getProperties()), function(key) {
                   if(!foundFields[key]) {
                       foundFields[key] = 0;
                   }
                   
                   foundFields[key];
               });
            });
            
            // Pick the 4 most common
            me.availableFields = _.keys(foundFields).sort(function(a,b){
                return foundFields[a] - foundFields[b];
            });
            
            me.listFields = me.availableFields.slice(0,4);
            
            $("#mor_data_listfields").val(me.listFields.join(","));
        }
    };
	
})(jQuery);