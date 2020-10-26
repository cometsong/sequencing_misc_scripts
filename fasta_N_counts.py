import sys
import re

def read_fasta(fasta_file):
    """Yield generator of header, seq lines in fasta file."""
    name, seq = None, []
    try:
        with open(fasta_file, "r") as fh:
            for line in fh:
                line = line.rstrip()
                if line.startswith(">"):
                    if name: yield (name, ''.join(seq))
                    name, seq = line, []
                else:
                    seq.append(line)
            if name: yield (name, ''.join(seq))
    except Exception as e:
        print(f'Error reading fasta sequence. {e.args}')
        raise e



def write_out_file(contents, filename, mode='w'):
    """Write contents to outfile directly.
    Default mode is truncate/create new file; pass mode='a' if append to existing.
    """
    try:
        with open(filename, mode) as f:
            f.write(contents)
    except Exception as e:
        print(f'Error: {e}')
        return None
    else:
        return filename


if __name__ == '__main__':
    if len(sys.argv) >= 2:

        """Count lengths of N sets."""
        fasta_file = sys.argv[1]
        ns = re.compile('N+')
        for head, seq in read_fasta(fasta_file):
            for n in re.finditer('N+', seq):
                print(len(n.group(0)))

    else:
        print("Usage:  {} file.fasta".format(__file__))
