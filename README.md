FR-HIT
===========
FR-HIT is an efficient fragment recruitment algorithm for next generation sequences against microbial reference genomes.
It produces similar sensitivity of BLASTN, but runs at a 100 times higher speed. 
We applied FR-HIT, BLAST and several other alignment programs to recruit an Illumina dataset of 1 million 75-bp reads 
selected from a recent human gut microbiome study (Qin J, et al. Nature 2010, 464:59) to 194 public human gut bacterial 
reference genomes. BLAST recruited 475,584 reads and found 6,134,663 alignments in 241.5 hours. 
Mapping program BWA used 0.1 hours, but only produced 212,699 recruitments. FR-HIT recruited 523,868 reads and 
identified 5,780,580 alignments in 1.8 hours.

Current version: 0.7

The current version supports:

    1. Multithreads parallel computing using OPENMP.
    2. PSL output format.
    3. Mask of low-complexity regions as lower cased sequences in reference database.
    4. E-value cutoff.

Three perl scripts:

    psl2sam.pl: transfer PSL format to SAM format.(This script is from samtools package)
    frhit2pairend.pl: analysis FR-HIT output to get pair-end alignment information
    binning-1.1.1: This program package performs taxonomy binning using output from FR-HIT. The algorithm is based on LCA (Lowest Common Ancestor).

Usage
-----

Usage:   fr-hit v0.7 [options]

        -a   <string>   reads file, *.fasta format
        -d   <string>   reference genome sequences file, *.fasta format
        -o   <string>   output recruitments file
        -e   <double>   e-value cutoff, default=10
        -u   <int>      mask out repeats as lower cased sequence to prevent spurious hits? 1: yes; 0: no; default=1
        -f   <int>      format control for output file,0:FR-HIT format; 1:PSL fromat, default=0
        -k   <int>      k-mer size (8<=k<=12), default=11
        -p   <int>      k-mer overlap of index (1<=p<-k), using small overlap for longer reads(454, Sanger), default=8
        -c   <int>      sequence identity threshold(%), default=75
        -g   <int>      use global or local alignment? 1:global; 0:local (need -m), default=0
        -w   <int>      minimal read length to use 2bp k-mer index step to 454 long reads, default=1000
        -m   <int>      minimal alignment coverage control for the read (g=0), default=30
        -l   <int>      length of throw_away_reads, default=20
        -t   <int>      maximum number of failed alingment attempts, default=20
        -r   [0,N]      how to report alignment hits, 0:all; N:the best top N hits for one read, default=0
        -n   <int>      do alignment for which chain? 0:both; 1:direct only; 2:complementary only. default=0
        -b   <int>      band_width of alignment, default=4
        -T   [0,N]      number of threads, default 1; with 0, all CPUs will be used
        -h   help

example:

        ./fr-hit -a 454reads-sample.fa -d 1000bacterialgenomes.fasta -c 90 -m 40 -w 120 -r 0 -o out.sop


The default output format of FR-HIT recruitment result file looks like:

        ReadName	ReadLength	E-value	AlignmentLength	Begin	End	Strand	Identity	ReferenceSequenceName	Begin	End

        1_lane2_1       75nt    8.3e-25 69      69      1       -       95.01%     Acidaminococcus_D21     486841311       486841379
        1_lane2_1       75nt    8.3e-25 69      69      1       -       95.23%     Ruminococcus_5_1_39B_FAA        3450573 3450641
        1_lane2_9       75nt    2.2e-25 64      1       64      +       98.81%     Acidaminococcus_D21     7901322 7901385
        1_lane2_9       75nt    2.2e-25 64      1       64      +       98.90%     Alistipes_putredinis_DSM_17216  1029618 1029681
        1_lane2_10      75nt    4.5e-23 72      1       72      +       93.32%     Acidaminococcus_D21     453948881       453948952
        1_lane2_10      75nt    4.5e-23 72      1       72      +       93.67%     Prevotella_copri_DSM_18205      3128442 3128513
        1_lane2_11      75nt    1.7e-22 74      75      2       -       91.21%     Acidaminococcus_D21     451839012       451839085
        1_lane2_11      75nt    1.7e-22 74      75      2       -       91.08%     Prevotella_copri_DSM_18205      1018573 1018646

FR-HIT supports PSL output format and users can also use psl2sam.pl to convert PSL format to SAM format.

Install
--------

FR-HIT has no any dependency, so just clone FR-HIT repo, and build the fr-hit binary:

        git clone https://github.com/Beifang/fr-hit.git
        cd fr-hit
        make


Now you can put the resulting binary where your `$PATH` can find it. If you have supermissions, then
I recommend dumping it in the system directory for locally compiled packages:

        sudo mv fr-hit /usr/local/bin/


