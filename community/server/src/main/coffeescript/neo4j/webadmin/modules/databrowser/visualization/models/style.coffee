###
Copyright (c) 2002-2018 "Neo Technology,"
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
    
    LABEL_PATTERN_TOOLTIP = """You can use placeholders in the label.<br/>
{id} for node id<br/>
{propertyname} or {prop.propertyname} for properties.<br/>
{props} for all properties.<br/><br/>
<b>Truncate values</b><br/>
{bigproperty|truncate:10}<br/><br/>
<b>Use first matching property</b><br/>
{name,title,id}<br/><br/>
<b>Multiline labels</b><br/>
Use ";" to create multiline labels.<br/><br/>
<b>Example</b><br/>
{id};{description|truncate:20}..
"""
    
    class BoxOrCircleStyleForm extends forms.ModelForm
      createFields : () ->
        {
          shapeColor : new forms.ColorField("Background")
          labelColor : new forms.ColorField("Label color")
        }
    
    class IconForm extends forms.ModelForm
      createFields : () ->
        {
          iconUrl : new forms.ImageURLField("Icon url", {
            imageUrls : [
              "img/icons/glyphish/21-skull.png"
              "img/icons/glyphish/07-map-marker.png"
              "img/icons/glyphish/08-chat.png"
              "img/icons/glyphish/10-medical.png"
              "img/icons/glyphish/11-clock.png"
              "img/icons/glyphish/12-eye.png"
              "img/icons/glyphish/13-target.png"
              "img/icons/glyphish/14-tag.png"
              "img/icons/glyphish/18-envelope.png"
              "img/icons/glyphish/19-gear.png"
              "img/icons/glyphish/21-skull.png"
              "img/icons/glyphish/22-skull-n-bones.png"
              "img/icons/glyphish/23-bird.png"
              "img/icons/glyphish/24-gift.png"
              "img/icons/glyphish/25-weather.png"
              "img/icons/glyphish/26-bandaid.png"
              "img/icons/glyphish/27-planet.png"
              "img/icons/glyphish/28-star.png"
              "img/icons/glyphish/29-heart.png"
              "img/icons/glyphish/52-pine-tree.png"
              "img/icons/glyphish/53-house.png"
              "img/icons/glyphish/56-cloud.png"
              "img/icons/glyphish/64-zap.png"
              "img/icons/glyphish/71-compass.png"
              "img/icons/glyphish/76-baby.png"
              "img/icons/glyphish/82-dog-paw.png"
              "img/icons/glyphish/84-lightbulb.png"
              "img/icons/glyphish/90-life-buoy.png"
              "img/icons/glyphish/94-pill.png"
              "img/icons/glyphish/99-umbrella.png"
              "img/icons/glyphish/102-walk.png"
              "img/icons/glyphish/109-chicken.png"
              "img/icons/glyphish/110-bug.png"
              "img/icons/glyphish/111-user.png"
              "img/icons/glyphish/112-group.png"
              "img/icons/glyphish/113-navigation.png"
              "img/icons/glyphish/114-balloon.png"
              "img/icons/glyphish/116-controller.png"
              "img/icons/glyphish/119-piggy-bank.png"
              "img/icons/glyphish/132-ghost.png"
              "img/icons/glyphish/133-ufo.png"
              "img/icons/glyphish/134-viking.png"
              "img/icons/glyphish/136-tractor.png"
              "img/icons/glyphish/145-persondot.png"
              "img/icons/glyphish/170-butterfly.png"
              "img/icons/glyphish/171-sun.png"
              "img/icons/glyphish/195-barcode.png"
              "img/icons/glyphish/196-radiation.png"
            ]
          })
          labelColor : new forms.ColorField("Label color")
        }
    
    class NodeStyleForm extends forms.ModelForm
      
      createFields : () -> 
        {
          shape : new forms.FormChooserField "Show as",
            box  : new forms.FormChooserOption("Box",    new BoxOrCircleStyleForm(model:@model))
            dot  : new forms.FormChooserOption("Circle", new BoxOrCircleStyleForm(model:@model))
            icon : new forms.FormChooserOption("Icon",   new IconForm(model:@model))
          
          label : new forms.FieldSet
            labelPattern : new forms.TextField("Label", {tooltip:LABEL_PATTERN_TOOLTIP})
            labelSize : new forms.NumberField("Font size")
        }

    exports.NodeStyle = class NodeStyle extends LocalModel
      
      defaults :
        type : 'node' # For deserialization
        
        shape : 'box'
        
        shapeColor : '#000000'
        
        labelFont : "monospace"
        labelSize : 10
        labelColor : "#eeeeee"
        labelPattern : "{id}"
        
      getViewClass : -> NodeStyleForm
      
      getLabelPattern : -> @get 'labelPattern'
        
      applyTo : (visualNode) ->
        visualNode.style ?= {} 
        
        shapeColor = new RGBColor @get 'shapeColor'
        
        visualNode.style.shapeStyle = 
          fill : shapeColor.toHex()
          shape : @get "shape"
        
        visualNode.style.iconUrl = @get "iconUrl"
          
        visualNode.style.labelStyle = 
          font : @get 'labelFont'
          color : @get 'labelColor'
          size : @get 'labelSize'
          
        labelCtx = @getLabelCtx(visualNode)
        
        labelPattern = @getLabelPattern()
        if labelPattern != null and labelPattern.length > 0
          visualNode.style.labelText = Nano.compile @getLabelPattern(), labelCtx
        else
          visualNode.style.labelText = ""
        
      getLabelCtx : (visualNode) ->
        ctx = {
          id : "N/A",
          props : "",
          prop : {}
        }
        if visualNode.neoNode
          for k,v of visualNode.neoNode.getProperties()
            ctx[k] = JSON.stringify(v)
          ctx['id'] = visualNode.neoNode.getId()
          ctx['props'] = JSON.stringify(visualNode.neoNode.getProperties())
          ctx['prop'] = visualNode.neoNode.getProperties()
        ctx


    exports.GroupStyle = class GroupStyle extends NodeStyle
       
      defaults :
        type : 'group' # For deserialization
        
        shape : 'dot' 
        
        shapeColor : '#590101'
        
        labelSize : 10
        labelFont : "monospace"
        labelColor : "#eeeeee"
        labelPattern : "{count};nodes"
        
      getLabelCtx : (visualNode) ->
        return {
          count : visualNode.group.nodeCount
        }
        
    return exports
)
