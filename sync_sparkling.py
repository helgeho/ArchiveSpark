import os
import sys

sparkling_src = sys.argv[1]
sparkling_path = "./src/main/scala/org/archive/archivespark/sparkling"

sparkling_src_namespace = sys.argv[2]
sparkling_namespace = "org.archive.archivespark"

def iterate_sparkling(sub):
    for filename in os.listdir(sparkling_path + sub):
        file_path = sub + "/" + filename
        if os.path.isdir(sparkling_path + file_path):
            iterate_sparkling(file_path)
        else:
            if os.path.exists(sparkling_src.replace("\\", "/") + file_path):
                src = open(sparkling_src.replace("\\", "/") + file_path, 'r')
                dst = open(sparkling_path + file_path, 'w')
                for line in src:
                    dst.write(line.replace(sparkling_src_namespace, sparkling_namespace))
                src.close()
                dst.close()
            else:
                os.remove(sparkling_path + file_path)

iterate_sparkling("")