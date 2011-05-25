#!/usr/bin/python

from __future__ import with_statement
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import subprocess

import os, cgi, re

FEATURES_FOLDER = "src/features"

SCENARIO_TEMPLATE = """
<li>
  %(name)s <input type="submit" name="action:%(feature)s:%(lineno)d" value="Run" />
</li>
"""

FEATURE_TEMPLATE = """
<li>
  <h3>%(name)s <input type="submit" name="action:%(feature)s" value="Run" /></h3>
  <ul class="scenarios">
    %(scenarios)s
  </ul>
</li>
"""

PAGE_TEMPLATE = """
<!DOCTYPE html>
<html>
  <head>
    <title>Cucumber test runner</title>
    <style>
      body {
        color: #333;
        font-size: 90%%;
        font-family: sans-serif;
      }

      ul {
        list-style:none;
        padding-left:5px;
        font-size:10px;
        line-height:26px;
      }

      ul li {
        display:block;
        height:26px;
        border-top:1px solid #eee;
        clear:both;
      }

      ul.scenarios li:nth-child(2n+1) {
        background:#eeffff;
      }
    
      ul li input {
        float:right;
      }

      #features > li {
        height:50px; 
      }

      #feature-container {
        width:50%%;
        float:left;
      }

      #output-container {
        width:45%%;
        float:left;
        margin-left:4%%;
      }
      #output-container textarea {
        height: 400px;
        width:100%%;
      } 
    </style>
  </head>
  <body>
    <div id="feature-container">
      <h3>Features:</h3>
      <form action="" method="POST">
        <p>
           <input type="checkbox" name="external-server" %(external_checkbox)s /> run with external server 
           <input type="checkbox" name="use-dev-html" %(dev_html_checkbox)s /> use dev.html (faster roundtrip)  
           <input type="submit" name="action:%(features_folder)s" value="Run all" style="float:right;"/>
        </p>
        <p>
           <input type="checkbox" name="bare-bones" %(bare_bones_checkbox)s /> return bare-bones output
        </p>
        <ul id="features">
          %(features)s
        </ul>
      </form>
    </div>
    <div id="output-container">
      <h3>Output:</h3>
      <textarea>%(console_output)s</textarea>
    </div>
  </body>
</html>
"""

def clean_output(output, bare_bones=False):

    output = output.replace(u"", "")
    if bare_bones:
        lines = []
        for line in output.split("\n"):
            if line.endswith("[NOT SUPPORTED]"): # Cucumber finding strange files
                continue
            if line.startswith("[INFO] "):
                line = line[7:]
            
            if line.strip().startswith("*") and line.strip().endswith(".class"): # Cucumber listing class files
                continue

            line = re.sub("\[\d+m", "", line)
            line = re.sub("\s+#\s", " # ", line)
            lines.append(line)

        output = "\n".join(lines)

    return output

class Handler(BaseHTTPRequestHandler):

    def parse_feature_file(self, path):
      
      feature_name = path
      scenarios = []

      with open(path) as f:
        line = f.readline()
        lineno = 1
        while (line != ""):
          
          if line.strip().startswith("Feature:"):
            feature_name = line.strip()[9:].strip()

          if line.strip().startswith("Scenario:"):
            scenarios.append( (line.strip()[9:].strip(), lineno) )

          lineno += 1
          line = f.readline()

      return (feature_name, path, sorted(scenarios, key=lambda f : f[0]))

    def get_features(self):
        
        features = []
        for root, subFolders, files in os.walk(FEATURES_FOLDER):
          for file in files:
            features.append( self.parse_feature_file(os.path.join(root,file)) )

        return sorted(features,key=lambda f : f[0])

    def get_features_html(self):
        features_html = []
        for name, feature, scenarios in self.get_features():            
            scenarios_html = []
            for scenario, lineno in scenarios:
              scenarios_html.append(SCENARIO_TEMPLATE % { "name":scenario,"lineno":lineno, "feature": feature})

            features_html.append( FEATURE_TEMPLATE % {"name":name, "feature": feature, "scenarios":"".join(scenarios_html) }) 

        return "".join(features_html)

    def do_GET(self, 
               console_output="", 
               external_checkbox="", 
               dev_html_checkbox="checked=\"checked\"",
               bare_bones_checkbox="checked=\"checked\""):
        
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

        self.wfile.write(PAGE_TEMPLATE % {"features":self.get_features_html(), 
                                          "console_output":console_output,
                                          "external_checkbox":external_checkbox,
                                          "dev_html_checkbox":dev_html_checkbox,
                                          "features_folder":FEATURES_FOLDER,
                                          "bare_bones_checkbox":bare_bones_checkbox,})

    def do_POST(self):
        
        form = cgi.FieldStorage(
            fp=self.rfile, 
            headers=self.headers,
            environ={'REQUEST_METHOD':'POST',
                     'CONTENT_TYPE':self.headers['Content-Type'],
                     })

        feature = ""
        external_server = False
        use_dev_html = False
        bare_bones = False
        for field in form.keys():
            if field == "external-server":
              external_server = True

            if field == "use-dev-html":
              use_dev_html = True
            
            if field == "bare-bones":
              bare_bones = True

            if field.strip().startswith("action:"):
              feature = field.strip()[7:]

        command = ["mvn", "integration-test", "-Pneodev", "-Dtests=web", "-DcukeArgs=%s" % feature]
        if external_server:
          command.append("-DtestWithExternalServer=true")
        if use_dev_html:
          command.append("-DtestWithDevHtmlFile=true")
        
        print " ".join(command)

        p=subprocess.Popen(command, stdout=subprocess.PIPE)
        out, err=p.communicate()

        out = clean_output(out, bare_bones)

        external_checkbox = "checked=\"checked\"" if external_server else ""
        dev_html_checkbox = "checked=\"checked\"" if use_dev_html else ""
        bare_bones_checkbox = "checked=\"checked\"" if bare_bones else ""
        self.do_GET(out, 
                    external_checkbox = external_checkbox,
                    dev_html_checkbox=dev_html_checkbox, 
                    bare_bones_checkbox=bare_bones_checkbox);
        
if __name__ == "__main__":
    try:
        s = HTTPServer(('',8666), Handler)
        print "Starting server at http://localhost:8666/"
        s.serve_forever()
    except Exception, e:
        print e
        s.socket.close()
