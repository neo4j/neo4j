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
  ['./nodeStyleTemplate'
   'ribcage/LocalModel',
   'ribcage/ui/Nano',
   'ribcage/forms',
   'ribcage/View'
   'lib/rgbcolor'], 
  (template, LocalModel, Nano, forms, View) ->
  
    class NodeStyleView extends forms.ModelForm
      
      fields :
      
        shape : new forms.FieldSet("Shape",{
          shapeColor : new forms.ColorField("Background")
          strokeColor : new forms.ColorField("Border color")
        })
        label : new forms.FieldSet("Label",{
          labelColor : new forms.ColorField("Color")
          labelPattern : new forms.TextField("Text", {tooltip:'You can use placeholders here, {id} for node id, {prop.PROPERTYNAME} for properties.'})
        })
      save : () ->
        neo4j.log @model

    class NodeStyle extends LocalModel
      
      defaults :
        type : 'node' # For deserialization
        
        shape : 'box'
        shapeOpacity : 0.9
        shapeColor : '#000000'
        strokeColor : '#333333'
        
        labelFont : "monospace"
        labelColor : "#eeeeee"
        labelPattern : "{id}: {prop.name}"
        
      getViewClass : -> NodeStyleView
      
      getLabelPattern : -> @get 'labelPattern'
        
      applyTo : (visualNode) ->
        visualNode.style ?= {} 
        
        shapeColor = new RGBColor @get 'shapeColor'
        strokeColor = new RGBColor @get 'strokeColor'
        
        visualNode.style.shape = @get 'shape'
        visualNode.style.shapeStyle = 
          fill : shapeColor.toHex()
          stroke : strokeColor.toHex()
          
        visualNode.style.labelStyle = 
          font : @get 'labelFont'
          color : @get 'labelColor'
          
        labelCtx = 
          id : visualNode.neoNode.getId()
          prop : visualNode.neoNode.getProperties()
        
        visualNode.style.labelText = Nano.compile @getLabelPattern(), labelCtx
)
