
/**
 * Handles adding, editing and removing properties on nodes and relationships.
 */
wa.components.data.PropertyEditor = (function($) { 

	var me = {};
	
	/**
	 * The key of the current field being edited, if any.
	 */
	me.currentEditKey = null;
	
	me.dataCore = wa.components.data.DataBrowser;
	
	//
	// INTERNALS
	//
	
	me.propertyUrl = function(key) {
		var it = me.dataCore.getItem();
		return it.property.replace("{key}",key);
	};
	
	me.addProperty = function(ev) {
		ev.preventDefault();
		var template = $(ev.target).parent().find(".mor_data_property_template");
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
			if( key !== null ) {
				neo4j.Web.del(me.propertyUrl(key));
			}
			$(ev.target).closest("tr").remove();
		}
	};
	
	me.propertyValueChanged = function(ev) {
		var key = me.getKey(ev.target);
		var value = $(ev.target).val();
		if( key !== null ) {
		    neo4j.Web.put(me.propertyUrl(key), value);
		}
	};
	
	me.propertyKeyChanged = function(ev) {
		var oldKey = me.currentEditKey;
		var key = $(ev.target).val();
		var value = me.getValue(ev.target);
		
		if( key != oldKey && value !== null) {
			// Key has changed
			if( oldKey !== null && oldKey.length > 0 ) {
				// Delete old property
			    neo4j.Web.del(me.propertyUrl(oldKey));
			}
			
			neo4j.Web.put(me.propertyUrl(key), value);
		}
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
		var val = $(valueField).closest("tr").find("input.mor_data_key_input").val();
		if(val.length > 0) {
			return val;
		} else {
			return null;
		}
	};
	
	/**
	 * Get the string value for a given key field. Returns null if value is not set.
	 */
	me.getValue = function(valueField) {
		var val = $(valueField).closest("tr").find("input.mor_data_value_input").val();
		if(val.length > 0) {
			return val;
		} else {
			return null;
		}
	};
	
	//
	// ADD EVENT LISTENERS
	//
	
	$("a.mor_data_add_property").live("click", me.addProperty);
	$("a.mor_data_remove_property").live("click", me.removeProperty);
	
	$("input.mor_data_value_input, input.mor_data_key_input").live("focus", me.propertyFieldFocused);
	$("input.mor_data_value_input, input.mor_data_key_input").live("blur", me.unfocusFields);
	
	$("input.mor_data_value_input").live("change", me.propertyValueChanged);
	$("input.mor_data_key_input").live("change", me.propertyKeyChanged);
	
	return {};
	
})(jQuery);