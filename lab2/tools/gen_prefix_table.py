import pprint

with open('../c-test/prefixes.h', 'w') as outfile:
    outfile.write("#pragma once\n\nconst bool prefixes_3[] = {\n")
    prefixes_3 = []
    for key in range(0,9|9<<4):
        prefixes_3.append('false')
    print(len(prefixes_3))
    with open('../valid_prefixes_3.txt', 'r') as f:
        for line in f:
            l = list(map(int, list(line.strip())))
            key = l[1] | (l[2]<<4)
            prefixes_3[key] = 'true'
            #
            #if l[0] not in prefixes_3:
            #    prefixes_3[l[0]]={}

            #if l[1] not in prefixes_3[l[0]]:
            #    prefixes_3[l[0]][l[1]]={}
            #prefixes_3.update({l[0]: (})[l[0]][l[1]][l[2]] = True;

    for i in prefixes_3:
        outfile.write("{0},\n".format(i))

    outfile.write("};\n\nconst bool prefixes_4[] = {\n")
    prefixes_4 = []
    for key in range(0,9|9<<4|9<<8):
        prefixes_4.append('false')
    print(len(prefixes_4))
    with open('../valid_prefixes_4.txt', 'r') as f:
        for line in f:
            l = list(map(int, list(line.strip())))
            key = l[1]|(l[2]<<4)|(l[3]<<8)
            prefixes_4[key] = 'true'

    for i in prefixes_4:
        outfile.write("{0},\n".format(i))

    outfile.write("};\n")