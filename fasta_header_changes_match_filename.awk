#!/bin/gawk -f

BEGIN {
    Usage = "Usage: script -v start=10 -v end=15 file.fasta > outfile.fasta \n \
         (with 'start'=first character in filename to use in header, 'end' being last.)"
    if (ARGC == 1) {
       print Usage
       exit
    }
    ORS = RS = ">"
    OFS = FS
}
{
    filename = basename(FILENAME)
    file_sub = substr(filename, start, end-start+1)
    #print file_sub, $0
}
FNR==1 { print $0 } # for some reason, awk prints the file_sub beforehand?!?!
FNR>1  { print file_sub, $0 }

function basename(    file, parts) {
    split(file, parts, "/")
    return parts[length(parts)] # length requires 'gawk'
}
