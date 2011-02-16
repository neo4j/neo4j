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
 * Handles adding, editing and removing properties on nodes and relationships.
 */
wa.components.data.PropertyEditor = (function($) { 

	var me = {};
	
	/**
	 * The key of the current field being edited, if any.
	 */
	me.currentEditKey = null;
	
	/**
	 * List of html elements that are erreneous due to
	 * duplicate keys.
	 */
	me.duplicateKeyFields = [];
	
	me.item = wa.components.data.DataBrowser.getItem;
	
	//
	// INTERNALS
	//
	
	me.propertyUrl = function(key) {
		var it = me.dataCore.getItem();
		return it.property.replace("{key}",key);
	};
	
	me.addProperty = function(ev) {
		ev.preventDefault();
		
		var template = $(ev.target).parent().parent().find(".mor_data_property_template");
		var propertyElement = template.clone();

		propertyElement.removeClass("mor_data_property_template");
		template.before(propertyElement);
		propertyElement.show();
		propertyElement.find("input.mor_data_key_input").focus();
	};
	
	me.removeProperty = function(ev) {
		ev.preventDefault();
		if( confirm("Are you sure?")) {
			var key = me.getKey(ev.target);
			
            me.showSavingSaveButton();
            if( key !== null ) {
                me.item().removeProperty(key);
            }
            me.item().save().then(me.showSavedSaveButton);
            $(ev.target).closest("ul").remove();
		}
	};
	
	me.propertyValueChanged = function(ev) {
		var key = me.getKey(ev.target);
		var value = me.getValue($(ev.target));
		
		if( key !== null && value !== null ) {
			me.showSavingSaveButton();
			me.item().setProperty(key, value);
			me.item().save().then(me.showSavedSaveButton);
		}
	};
	
	me.propertyKeyChanged = function(ev) {
		var oldKey = me.currentEditKey;
		var key = $(ev.target).val();
		var value = me.getValue($(ev.target).closest("ul").find("input.mor_data_value_input"));
		
		if( key != oldKey ) {

            // Did the previous key exist?
            if( oldKey !== null && oldKey.length > 0 && ! $(ev.target).hasClass("error") ) {
                // Delete old property
                me.item().removeProperty(oldKey);
            }
		    
		    if( me.item().hasProperty(key)) {
		        // Key already exists, verboten!
		        me.markKeyFieldAsDuplicate(ev.target);
		    } else {
                me.unmarkKeyFieldAsDuplicate(ev.target);
                
                // Do a run to see if this "unlocks" any fields marked
                // as duplicates.
                me.currentEditKey = null;
                _.each(me.duplicateKeyFields, function(field) {
                    me.propertyKeyChanged({target:field});
                });
                
                // Save
                if ( value !== null ) {
        			me.showSavingSaveButton();
        			
        			me.item().setProperty(key, value);
                    me.item().save().then(me.showSavedSaveButton);
                }
                
		    }
		}
	};
	
	me.markKeyFieldAsDuplicate = function(el) {
	    if( ! me.keyFieldIsMarkedAsDuplicate(el)) {
	        me.duplicateKeyFields.push(el);
	    }
	    $(el).addClass("error");
	};
	
	me.unmarkKeyFieldAsDuplicate = function(el) {
	    me.duplicateKeyFields = _.without(me.duplicateKeyFields, el);
	    neo4j.log(el);
	    $(el).removeClass("error");
	};
	
	me.keyFieldIsMarkedAsDuplicate = function(el) {
	    return _.indexOf(me.duplicateKeyField, el) != -1
	};
	
	me.propertyFieldFocused = function(ev) {
		me.unfocusFields();
		var el = $(ev.target);
		el.addClass("focused");
		
		if(el.hasClass("mor_data_key_input")) {
			me.currentEditKey = el.val();
		} else {
			me.currentEditKey = me.getKey(el);
		}
		
	};
	
	me.unfocusFields = function(ev) {
		$("input.mor_data_value_input, input.mor_data_key_input").removeClass("focused");
	};
	
	/**
	 * Get the string key for a given value field. Returns null if key is not set.
	 */
	me.getKey = function(valueField) {
		var keyEl = $(valueField).closest("ul").find("input.mor_data_key_input"),
		    val = keyEl.val();
		
		if(!me.keyFieldIsMarkedAsDuplicate(keyEl)) {
			return val;
		} else {
			return null;
		}
	};
	
	/**
	 * Get the string value for a given key field. Returns null if value is not set.
	 */
	me.getValue = function(el) {
		try {
		    val = JSON.parse(el.val());
		    if(_.isString(val) && val.length == 0) {
		        el.addClass("error");
		        return null;
		    } else {
		        el.removeClass("error");
		        return val;
		    }
		} catch(ex) {
		    el.addClass("error");
		    return null;
		}
	};
	
	me.showUnsavedSaveButton = function() {
		$("button.mor_data_save").removeAttr('disabled');
		$("button.mor_data_save").html("Not saved");
	};
	
	me.showSavedSaveButton = function() {
		$("button.mor_data_save").attr('disabled','disabled');
		$("button.mor_data_save").html("Saved");
	};
	
	me.showSavingSaveButton = function() {
		$("button.mor_data_save").attr('disabled','disabled');
		$("button.mor_data_save").html("Saving..");
	};
	
	//
	// ADD EVENT LISTENERS
	//
	
	$("button.mor_data_add_property").live("click", me.addProperty);
	$("a.mor_data_remove_property").live("click", me.removeProperty);
	
	$("input.mor_data_value_input, input.mor_data_key_input").live("focus", me.propertyFieldFocused);
	$("input.mor_data_value_input, input.mor_data_key_input").live("blur", me.unfocusFields);
	
	$("input.mor_data_value_input").live("change", me.propertyValueChanged);
	$("input.mor_data_key_input").live("change", me.propertyKeyChanged);
	$("input.mor_data_value_input, input.mor_data_key_input").live("keypress", me.showUnsavedSaveButton);
	
	return {};
	
})(jQuery);