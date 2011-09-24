###
Copyright (c) 2002-2011 "Neo Technology,"
Network Engine for Objects in Lund AB [http://neotechnology.com]

This file is part of Neo4j.

Neo4j is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
###

define(
  ['ribcage/LocalModel',
   'ribcage/ui/Nano',
   'ribcage/forms',
   'ribcage/View'
   'lib/rgbcolor'], 
  (LocalModel, Nano, forms, View) ->
    exports = {}
        
    class NodeStyleView extends forms.ModelForm
      
      fields :
      
        shape : new forms.FieldSet({
          shapeColor : new forms.ColorField("Background")
          labelColor : new forms.ColorField("Label color")
        })
        label : new forms.FieldSet({
          labelPattern : new forms.TextField("Label", {tooltip:'You can use placeholders here, {id} for node id, {prop.PROPERTYNAME} for properties. Use ";" to create multiline labels. Example: "id: {id};name: {prop.name}"'})
        })

    
    class GroupStyleView extends forms.ModelForm
      
      fields :
      
        shape : new forms.FieldSet({
          shapeColor : new forms.ColorField("Background")
        })
        label : new forms.FieldSet({
          labelColor : new forms.ColorField("Color")
          labelPattern : new forms.TextField("Text", {tooltip:'You can use {count} here to show the number of nodes in the group. Use ";" to create multiline labels.'})
        })
     

    exports.NodeStyle = class NodeStyle extends LocalModel
      
      defaults :
        type : 'node' # For deserialization
        
        shape : 'box'
        
        shapeColor : '#000000'
        
        labelFont : "monospace"
        labelColor : "#eeeeee"
        labelPattern : "{id}"
        
      getViewClass : -> NodeStyleView
      
      getLabelPattern : -> @get 'labelPattern'
        
      applyTo : (visualNode) ->
        visualNode.style ?= {} 
        
        shapeColor = new RGBColor @get 'shapeColor'
        #strokeColor = new RGBColor @get 'strokeColor'
        
        visualNode.style.shapeStyle = 
          fill : shapeColor.toHex()
          shape : @get "shape"
          
        visualNode.style.labelStyle = 
          font : @get 'labelFont'
          color : @get 'labelColor'
          
        labelCtx = @getLabelCtx(visualNode)
        visualNode.style.labelText = Nano.compile @getLabelPattern(), labelCtx
        
      getLabelCtx : (visualNode) ->
        return {
          id : if visualNode.neoNode then visualNode.neoNode.getId() else 'N/A'
          prop : if visualNode.neoNode then visualNode.neoNode.getProperties() else {}
        }


    exports.GroupStyle = class GroupStyle extends NodeStyle
       
      defaults :
        type : 'group' # For deserialization
        
        shape : 'dot' 
        
        shapeColor : '#590101'
        
        labelFont : "monospace"
        labelColor : "#eeeeee"
        labelPattern : "{count};nodes"
        
      getViewClass : -> GroupStyleView
        
      getLabelCtx : (visualNode) ->
        return {
          count : visualNode.group.nodeCount
        }
        
    return exports
)
