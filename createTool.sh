#!/usr/bin/env python
import os
import sys
import string

def createFromTemplate(toolName, ext, src_dir, dest_dir, test_dir = None):
  if toolName.endswith(ext):
    toolName = toolName[:len(ext)]
  if not os.path.isdir(dest_dir):
    os.mkdir(dest_dir)
  env = dict(os.environ)
  env['toolName'] = toolName
  src_fn = os.path.join(src_dir, 'Template' + ext)
  dst_fn = os.path.join(dest_dir, toolName + ext)

  with open(src_fn) as f, open(dst_fn, 'w') as out:
      t = string.Template(f.read())
      out.write(t.safe_substitute(env))  
  
  print 'Now edit the file: ' + dst_fn
  
  src_test_fn = os.path.join(src_dir, 'Template' + 'Test' + ext)
  if os.path.isfile(src_test_fn) and test_dir != None:
    if not os.path.isdir(test_dir):
      os.mkdir(test_dir)
    dst_test_fn = os.path.join(test_dir, toolName + 'Test' + ext)
    
    with open(src_test_fn) as f, open(dst_test_fn, 'w') as out:
        t = string.Template(f.read())
        out.write(t.safe_substitute(env))
    print 'And: ' + dst_test_fn    
    

def main():
  iarg = 1
  if sys.argv[1] in ['python', 'scala', 'java']:
    toolType = sys.argv[iarg]
    iarg += 1
  else:
    toolType = 'scala'
  
  toolName = sys.argv[iarg]
  
  if toolType == 'scala':
    createFromTemplate(toolName, 
      '.scala', 
      "templates/scala", 
      "src/main/scala/nl/utwente/bigdata",
      test_dir="src/test/scala/nl/utwente/bigdata"
    )
  elif toolType == 'python':
    createFromTemplate(toolName, '.py', 
      "templates/python", 
      "src/main/python",
      test_dir="src/main/python"
    )
  elif toolType == 'java':
    createFromTemplate(toolName, '.java', 
      "templates/java", 
      "src/main/java/nl/utwente/bigdata",
      test_dir="src/test/java/nl/utwente/bigdata"
    )

if __name__ == '__main__':
  main()