Set args = WScript.Arguments
Set s= CreateObject("Shell.Application")
s.NameSpace(args.Item(1)).CopyHere s.NameSpace(args.Item(0)).Items.Item(0).GetFolder.Items, 256+16