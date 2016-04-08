MetaSpark
===========
MetaSpark is an efficient fragment recruitment algorithm for next generation sequences against microbial reference genomes.


Usage
-----

Usage:   MetaSpark [options]

| Parameter name | Parameter type | Parameter meaning |
| ----- | :---- | :----- |
| --read |string	| reads file |
| -- ref | 	string |	reference genome sequences file |
| --refindex |	string |	reference genome sequences index file |
| --result | 	string |	output recruitments file |
| --identity |	int | 	sequence identity threshold(%), default=75 (-c) |
| --aligment |	int |	minimal alignment coverage control for the read (g=0), default=30 (-m) |
| --evalue | 	double |	e-value cutoff, default=10 (-e) |
| --kmersize |	int |	k-mer size (8<=k<=12), default=11 (-k) |


example:

        spark-submit --class com.ynu.MetaSpark --master spark://{spark master address}:{port} --name {app name} {MetaSpark jar file} --read {read file path on HDFS} --ref {reference file path on HDFS} --result {result store path}  --identity 90 --aligment 40


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

MetaSpark is build using Apache Maven. To build MetaSpark, run:

        git clone https://github.com/zhouweiyg/metaspark.git
        cd metaspark
        mvn compile package




