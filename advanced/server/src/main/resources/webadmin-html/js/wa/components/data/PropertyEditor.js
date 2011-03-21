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
	
	me.tooltips = {};
	
	me.item = wa.components.data.DataBrowser.getItem;
	
	//
	// INTERNALS
	//
	
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
		    if( val === null) {
		        el.addClass("error");
		        me.showTooltip(el, "Null values are not allowed.");
		        return null;
		    } else if (me.isMap(val)) {
		        el.addClass("error");
		        me.showTooltip(el, "Maps are not supported property values.");
                return null;
		    } else if ( _(val).isArray() && ! me.isValidArray(val) ) {
                el.addClass("error");
                me.showTooltip(el, "Only arrays with one type of values, and only primitive types, is allowed.");
                return null;
		    } else {
		        el.removeClass("error");
		        me.hideTooltip(el);
		        return val;
		    }
		} catch(ex) {
		    if(me.shouldBeConvertedToString(el.val())) {
		        var value = el.val();
		        el.val("\""+ el.val() +"\"");
		        me.showTooltip(el, "Your input has been automatically converted to a string.", 3000);
                el.removeClass("error");
                return value;
		    } else {
    		    el.addClass("error");
                me.showTooltip(el, "This does not appear to be a valid JSON value.");
    		    return null;
		    }
		}
	};
	
	me.isMap = function(val) {
	    return JSON.stringify(val).indexOf("{") === 0;
	};
	
	me.shouldBeConvertedToString = function(val) {
        return /^[a-z0-9-_\/\\\(\)#%\&!$]+$/i.test(val);
    };
	
	me.isValidArray = function(val) {
	    if(val.length == 0) {
	        return true;
	    }
	    
	    var validType = _.isString(val[0]) ? _.isString : 
	                        _.isNumber(val[0]) ? _.isNumber :
	                            _.isBoolean(val[0]) ? _.isBoolean : false;
	    if(validType === false) {
	        return false;
	    }
	    
	    for(var i=1,l=val.length;i<l;i++) {
	        if(!validType(val[i])) {
	            return false;
	        }
	    }
	    
	    return true;
	};
	
	me.showTooltip = function(el, message, timeout) {
	    me.getTooltipFor(el).show(message, el, timeout);  
	};
	
	me.hideTooltip = function(el) {
	    me.getTooltipFor(el).hide();
	};
	
	me.getTooltipFor = function(el) {
	    if( !me.tooltips[el] ) {
	        me.tooltips[el] = new wa.ui.Tooltip({
	            position : "right",
	            hideOnMouseOut : false
	        });
	    }
	    return me.tooltips[el];
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