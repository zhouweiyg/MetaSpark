MetaSpark
===========
MetaSpark is an efficient fragment recruitment algorithm for next generation sequences against microbial reference genomes.

Install
--------

MetaSpark is build using Apache Maven. To build MetaSpark, run:

        git clone https://github.com/zhouweiyg/metaspark.git
        cd metaspark
        mvn compile package

You will get a jar file in target folder if you package the source file successfully. Then, you can run MetaSpark.

Usage
--------

Usage:   MetaSpark [options]

| Parameter name | Parameter type | Parameter meaning |
| ----- | :---- | :----- |
| --read |string	| reads file |
| --ref | 	string |	reference genome sequences file |
| --refindex |	string |	reference genome sequences index file |
| --result | 	string |	output recruitments file |
| --identity |	int | 	sequence identity threshold(%), default=75 (-c) |
| --aligment |	int |	minimal alignment coverage control for the read (g=0), default=30 (-m) |
| --evalue | 	double |	e-value cutoff, default=10 (-e) |
| --kmersize |	int |	k-mer size (8<=k<=12), default=11 (-k) |


The default output format of MetaSpark recruitment result file looks like:

        ReadNumber	ReadLength	E-value	AlignmentLength	Begin	End	Strand	Identity	Begin	End  ReferenceSequenceName

        1	75nt	4.7e-25	69	1	-	95.65%	3450573	3450641	Ruminococcus_5_1_39B_FAA
        9	75nt	1.2e-25	1	64	+	98.44%	1029618	1029681	Alistipes_putredinis_DSM_17216
        10	75nt	2.5e-23	1	72	+	93.06%	3128442	3128513	Prevotella_copri_DSM_18205
        11	75nt	9.6e-23	75	2	-	91.89%	1018573	1018646	Prevotella_copri_DSM_18205
        14	75nt	1.0e-07	4	45	+	90.48%	301211	301252	Bacteroides_capillosus_ATCC_29799
        17	75nt	1.6e-28	69	1	-	98.55%	133030	133098	Bacteroides_vulgatus_ATCC_8482
        17	75nt	1.6e-28	69	1	-	98.55%	1718708	1718776	Bacteroides_D4
        17	75nt	1.6e-28	69	1	-	98.55%	601790	601858	Bacteroides_9_1_42FAA


Run MetaSpark:
--------
MetaSpark is based on Spark platform and it's data is stored in Hadoop HDFS, you should upload the reads and reference file to the HDFS cluster before you run the MetaSpark programming.  

        spark-submit --class com.ynu.MetaSpark --master spark://{spark master address}:{port} --name {app name} {MetaSpark jar file} --read {read file path on HDFS} --ref {reference file path on HDFS} --result {result store path}  --identity 90 --aligment 40

MetaSpark also provide a function to create reference index and store it to HDFS, so you can save a lots of time if you run the test with the same reference file. 

        spark-submit --class com.ynu.CreateRefIndexToHDFS --master spark://{spark master address}:{port} --name {app name} {MetaSpark jar file} --ref {reference file path on HDFS} --kmersize 11
        
After you create the reference index, you can use it in the new test.

        spark-submit --class com.ynu.MetaSpark --master spark://{spark master address}:{port} --name {app name} {MetaSpark jar file} --read {read file path on HDFS} --ref {reference file path on HDFS} --refindex {reference index file path on HDFS} --result {result store path}  --identity 90 --aligment 40

