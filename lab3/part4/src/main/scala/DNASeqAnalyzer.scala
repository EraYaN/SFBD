/* 
 * Copyright (c) 2015-2016 TU Delft, The Netherlands.
 * All rights reserved.
 * 
 * You can redistribute this file and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This file is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * 
 * Authors: Hamid Mushtaq
 *
*/
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.broadcast.Broadcast

import sys.process._
import org.apache.spark.scheduler._

import java.io._
import java.nio.file.{Paths, Files}
import java.net._
import java.util.Calendar

import scala.sys.process.Process
import scala.io.Source
import scala.collection.JavaConversions._
import scala.util.Sorting._

import tudelft.utils.ChromosomeRange
import tudelft.utils.DictParser
import tudelft.utils.Configuration
import tudelft.utils.SAMRecordIterator

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.HashPartitioner

import collection.mutable.HashMap
import collection.mutable.ArrayBuffer

import htsjdk.samtools._

object DNASeqAnalyzer {
final val MemString = "-Xmx2048m"
final val RefFileName = "ucsc.hg19.fasta"
final val SnpFileName = "dbsnp_138.hg19.vcf"
final val ExomeFileName = "gcat_set_025.bed"
val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("sparkListener.txt"), "UTF-8"))
//////////////////////////////////////////////////////////////////////////////
def samRead(outFileName: String): Array[(Int, SAMRecord)] =
{
	val bwaKeyValues = new BWAKeyValues(outFileName)
	bwaKeyValues.parseSam()
	val kvPairs: Array[(Int, SAMRecord)] = bwaKeyValues.getKeyValuePairs()

	//new File(outFileName).delete
	//Seq("rm", outFileName).lines

	return kvPairs
}

def writeToBAM(fileName: String, samRecordsSorted: Array[SAMRecord], bcconfig: Broadcast[Configuration]): ChromosomeRange =
{
	  val config = bcconfig.value
	  val header = new SAMFileHeader()
	  header.setSequenceDictionary(config.getDict())
	  val outHeader = header.clone()
	  outHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
	  val factory = new SAMFileWriterFactory();
	  val writer = factory.makeBAMWriter(outHeader, true, new File(fileName));

	  val r = new ChromosomeRange()
	  val input = new SAMRecordIterator(samRecordsSorted, header, r)
	  while (input.hasNext()) {
		val sam = input.next()
		writer.addAlignment(sam);
	  }
	  writer.close();

	  return r
}

def compareSAMRecords(a: SAMRecord, b: SAMRecord) : Int = 
{
	if(a.getReferenceIndex == b.getReferenceIndex)
		return a.getAlignmentStart - b.getAlignmentStart
	else
		return a.getReferenceIndex - b.getReferenceIndex
}

def runCommand(cmd: Seq[String], stdOutFile: String, stdErrFile: String) {
  val stdoutStream = new File(stdOutFile)
  val stderrStream = new File(stdErrFile)
  val stdoutWriter = new PrintWriter(stdoutStream)
  val stderrWriter = new PrintWriter(stderrStream)
  val exitValue = cmd.!(ProcessLogger(stdoutWriter.println, stderrWriter.println))
  stdoutWriter.close()
  stderrWriter.close()
}

def variantCall(chrRegion: Int, samRecords: Array[SAMRecord], bcconfig: Broadcast[Configuration],
        bc_stdErrCleanSam: Broadcast[String], bc_stdErrMarkDuplicates: Broadcast[String],
        bc_stdErrAddOrReplaceReadGroups: Broadcast[String], bc_stdErrBuildBamIndex: Broadcast[String],
        bc_stdErrIntersect: Broadcast[String], bc_stdErrRealignerTargetCreator: Broadcast[String],
        bc_stdErrIndelRealigner: Broadcast[String], bc_stdErrBaseRecalibrator: Broadcast[String],
        bc_stdErrPrintReads: Broadcast[String], bc_stdErrHaplotypeCaller: Broadcast[String],
        bc_stdOutCleanSam: Broadcast[String], bc_stdOutMarkDuplicates: Broadcast[String],
        bc_stdOutAddOrReplaceReadGroups: Broadcast[String], bc_stdOutBuildBamIndex: Broadcast[String],
        bc_stdOutIntersect: Broadcast[String], bc_stdOutRealignerTargetCreator: Broadcast[String],
        bc_stdOutIndelRealigner: Broadcast[String], bc_stdOutBaseRecalibrator: Broadcast[String],
        bc_stdOutPrintReads: Broadcast[String], bc_stdOutHaplotypeCaller: Broadcast[String]
    ): Array[(Int, (Int, String))] =
{
	  val config = bcconfig.value
      val tmpFolder = config.getTmpFolder
      val toolsFolder = config.getToolsFolder
      val refFolder = config.getRefFolder
      val numOfThreads = config.getNumThreads

      // Following is shown how each tool is called. Replace the X in regionX with the chromosome region number (chrRegion). 
      //    You would have to create the command strings (for running jar files) and then execute them using the Scala's process package. More 
      //    help about Scala's process package can be found at http://www.scala-lang.org/api/current/index.html#scala.sys.process.package.
      //    Note that MemString here is -Xmx14336m, and already defined as a constant variable above, and so are reference files' names.

      val samRecordsSorted = samRecords.sortWith{case(first, second) => compareSAMRecords(first, second) < 0}

      val p1 = tmpFolder + s"/region$chrRegion-p1.bam"
      val p2 = tmpFolder + s"/region$chrRegion-p2.bam"
      val p3 = tmpFolder + s"/region$chrRegion-p3.bam"
      val p3_metrics = tmpFolder + s"/region$chrRegion-p3-metrics.txt"
      val regionFile = tmpFolder + s"/region$chrRegion.bam"

      // SAM records should be sorted by this point
      val chrRange = writeToBAM(p1, samRecordsSorted, bcconfig)

      // Picard preprocessing
      //    java MemString -jar toolsFolder/CleanSam.jar INPUT=tmpFolder/regionX-p1.bam OUTPUT=tmpFolder/regionX-p2.bam
      var command = Seq("java", MemString, "-jar", toolsFolder + "CleanSam.jar", "INPUT=" + p1, "OUTPUT=" + p2)
      println(command)
      runCommand(command, "%s/chrReg%s.stdout".format(bc_stdOutCleanSam.value,chrRegion), "%s/chrReg%s.stderr".format(bc_stdErrCleanSam.value,chrRegion))
      //    java MemString -jar toolsFolder/MarkDuplicates.jar INPUT=tmpFolder/regionX-p2.bam OUTPUT=tmpFolder/regionX-p3.bam 
      //        METRICS_FILE=tmpFolder/regionX-p3-metrics.txt
      command = Seq("java", MemString, "-jar", toolsFolder + "MarkDuplicates.jar", "INPUT=" + p2, "OUTPUT=" + p3, "METRICS_FILE=" + p3_metrics)
      println(command)
      runCommand(command, "%s/chrReg%s.stdout".format(bc_stdOutMarkDuplicates.value,chrRegion), "%s/chrReg%s.stderr".format(bc_stdErrMarkDuplicates.value,chrRegion))
      //    java MemString -jar toolsFolder/AddOrReplaceReadGroups.jar INPUT=tmpFolder/regionX-p3.bam OUTPUT=tmpFolder/regionX.bam 
      //        RGID=GROUP1 RGLB=LIB1 RGPL=ILLUMINA RGPU=UNIT1 RGSM=SAMPLE1
      command = Seq("java", MemString, "-jar", toolsFolder + "AddOrReplaceReadGroups.jar", "INPUT=" + p3, "OUTPUT=" + regionFile, "RGID=GROUP1", "RGLB=LIB1", "RGPL=ILLUMINA", "RGPU=UNIT1", "RGSM=SAMPLE1")
      println(command)
      runCommand(command, "%s/chrReg%s.stdout".format(bc_stdOutAddOrReplaceReadGroups.value,chrRegion), "%s/chrReg%s.stderr".format(bc_stdErrAddOrReplaceReadGroups.value,chrRegion))
      //    java MemString -jar toolsFolder/BuildBamIndex.jar INPUT=tmpFolder/regionX.bam
      command = Seq("java", MemString, "-jar", toolsFolder + "BuildBamIndex.jar", "INPUT=" + regionFile)
      println(command)
      runCommand(command, "%s/chrReg%s.stdout".format(bc_stdOutBuildBamIndex.value,chrRegion), "%s/chrReg%s.stderr".format(bc_stdErrBuildBamIndex.value,chrRegion))
      //    delete tmpFolder/regionX-p1.bam, tmpFolder/regionX-p2.bam, tmpFolder/regionX-p3.bam and tmpFolder/regionX-p3-metrics.txt
      Seq("rm", p1, p2, p3, p3_metrics).lines

      // Make region file 
      val tmpBedFile = tmpFolder + s"tmp$chrRegion.bed"
      val bedFile = tmpFolder + s"bed$chrRegion.bed"
      //    val tmpBed = new File(tmpFolder/tmpX.bed)
      val tmpBed = new File(tmpBedFile)
      //    chrRange.writeToBedRegionFile(tmpBed.getAbsolutePath())
      chrRange.writeToBedRegionFile(tmpBed.getAbsolutePath())
      //    toolsFolder/bedtools intersect -a refFolder/ExomeFileName -b tmpFolder/tmpX.bed -header > tmpFolder/bedX.bed
      command = Seq(toolsFolder + "bedtools", "intersect", "-a", refFolder + ExomeFileName, "-b", tmpBedFile, "-header")
      runCommand(command,bedFile,"%s/chrReg%s.stderr".format(bc_stdErrIntersect.value,chrRegion))
      //    delete tmpFolder/tmpX.bed
      Seq("rm", tmpBedFile).lines
      

      // Indel Realignment 
      val intervalFile = tmpFolder + s"region$chrRegion.intervals"
      val region2File = tmpFolder + s"region$chrRegion-2.bam"
      val baiFile = tmpFolder + s"region$chrRegion.bai"
      //    java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T RealignerTargetCreator -nt numOfThreads -R refFolder/RefFileName 
      //        -I tmpFolder/regionX.bam -o tmpFolder/regionX.intervals -L tmpFolder/bedX.bed
      command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "RealignerTargetCreator", "-nt", numOfThreads, "-R", refFolder + RefFileName, "-I", regionFile, "-o", intervalFile, "-L", bedFile)
      println(command)
      runCommand(command, "%s/chrReg%s.stdout".format(bc_stdOutRealignerTargetCreator.value,chrRegion), "%s/chrReg%s.stderr".format(bc_stdErrRealignerTargetCreator.value,chrRegion))
      //    java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T IndelRealigner -R refFolder/RefFileName -I tmpFolder/regionX.bam 
      //        -targetIntervals tmpFolder/regionX.intervals -o tmpFolder/regionX-2.bam -L tmpFolder/bedX.bed
      command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "IndelRealigner", "-R", refFolder + RefFileName, "-I", regionFile, "-targetIntervals", intervalFile, "-o", region2File, "-L", bedFile)
      println(command)
      runCommand(command, "%s/chrReg%s.stdout".format(bc_stdOutIndelRealigner.value,chrRegion), "%s/chrReg%s.stderr".format(bc_stdErrIndelRealigner.value,chrRegion))
      //    delete tmpFolder/regionX.bam, tmpFolder/regionX.bai, tmpFolder/regionX.intervals
      Seq("rm", intervalFile, baiFile).lines //, intervalFile).lines
      //

      // Base quality recalibration 
      val regionTableFile = tmpFolder + s"region$chrRegion.table"
      val region3File = tmpFolder + s"region$chrRegion-3.bam"
      val bai2File = tmpFolder + s"region$chrRegion-2.bai"
      //    java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T BaseRecalibrator -nct numOfThreads -R refFolder/RefFileName -I 
      //        tmpFolder/regionX-2.bam -o tmpFolder/regionX.table -L tmpFolder/bedX.bed --disable_auto_index_creation_and_locking_when_reading_rods 
      //        -knownSites refFolder/SnpFileName
      command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "BaseRecalibrator", "-nct", numOfThreads, "-R", refFolder + RefFileName, "-I", region2File, "-o", regionTableFile, "-L", bedFile, "--disable_auto_index_creation_and_locking_when_reading_rods", "-knownSites", refFolder + SnpFileName)
      println(command)
      runCommand(command, "%s/chrReg%s.stdout".format(bc_stdOutBaseRecalibrator.value,chrRegion), "%s/chrReg%s.stderr".format(bc_stdErrBaseRecalibrator.value,chrRegion))
      //
      //    java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T PrintReads -R refFolder/RefFileName -I 
      //        tmpFolder/regionX-2.bam -o tmpFolder/regionX-3.bam -BSQR tmpFolder/regionX.table -L tmpFolder/bedX.bed 
      command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "PrintReads", "-R", refFolder + RefFileName, "-I", region2File, "-o", region3File, "-BQSR", regionTableFile, "-L", bedFile)
      println(command)
      runCommand(command, "%s/chrReg%s.stdout".format(bc_stdOutPrintReads.value,chrRegion), "%s/chrReg%s.stderr".format(bc_stdErrPrintReads.value,chrRegion))
      // delete tmpFolder/regionX-2.bam, tmpFolder/regionX-2.bai, tmpFolder/regionX.table
      Seq("rm", region2File, bai2File, regionTableFile).lines

      // Haplotype -> Uses the region bed file
      val vcfFile = tmpFolder + s"region$chrRegion.vcf"
      val bai3File = tmpFolder + s"region$chrRegion-3.bai"
      // java MemString -jar toolsFolder/GenomeAnalysisTK.jar -T HaplotypeCaller -nct numOfThreads -R refFolder/RefFileName -I 
      //        tmpFolder/regionX-3.bam -o tmpFolder/regionX.vcf  -stand_call_conf 30.0 -stand_emit_conf 30.0 -L tmpFolder/bedX.bed 
      //        --no_cmdline_in_header --disable_auto_index_creation_and_locking_when_reading_rods
      command = Seq("java", MemString, "-jar", toolsFolder + "GenomeAnalysisTK.jar", "-T", "HaplotypeCaller", "-nct", numOfThreads, "-R", refFolder + RefFileName, "-I", region3File, "-o", vcfFile, "-stand_call_conf", "30.0", "-stand_emit_conf", "30.0", "-L", bedFile, "--no_cmdline_in_header", "--disable_auto_index_creation_and_locking_when_reading_rods")
      println(command)
      runCommand(command, "%s/chrReg%s.stdout".format(bc_stdOutHaplotypeCaller.value,chrRegion), "%s/chrReg%s.stderr".format(bc_stdErrHaplotypeCaller.value,chrRegion))
      // delete tmpFolder/regionX-3.bam, tmpFolder/regionX-3.bai, tmpFolder/bedX.bed

      command = Seq("rm", region3File, bai3File, bedFile)
      println(command)
      command.lines

      var results = ArrayBuffer[(Int, (Int, String))]()
      val resultFile = Source.fromFile(vcfFile)
      for (line <- resultFile.getLines()) {
        if (!line.startsWith("#")) {
          val tabs = line.split("\t")
          var chrom = 0
          if (tabs(0) == "chrX") {
            chrom = 23
          } else {
            chrom = (tabs(0).filter(_.isDigit)).toInt
          }
          val pos = tabs(1).toInt
          results += ((chrom, (pos, line)))
        }
      }
      println("steady")
      results.toArray
}

def deleteFolder(folder: File) {
    var files = folder.listFiles();
    if(files!=null) { //some JVMs return null for empty dirs
        files.foreach({ f=>
            if(f.isDirectory()) {
                deleteFolder(f);
            } else {
                f.delete();
            }
        })
    }
    folder.delete();
}

def loadBalancer(weights: Array[(Int, Int)], vardensity: Array[(Int,(String,Int,Int))], numTasks: Int): ArrayBuffer[ArrayBuffer[Int]] = 
{
	var results = ArrayBuffer.fill(numTasks)(ArrayBuffer[(String,Int,Int)]())
	var sizes = ArrayBuffer.fill(numTasks)(0)


	for ((density, (chromosome,chromosomeIdx,partIdx)) <- vardensity.sorted.reverse) {
	  val region = sizes.zipWithIndex.min._2
	  sizes(region) += density
	  results(region) += (chromosome,chromosomeIdx,partIdx)
	}
	results
}

def getTimeStamp() : String =
{
	return "[" + new SimpleDateFormat("HH:mm:ss").format(Calendar.getInstance().getTime()) + "] "
}

def main(args: Array[String]) 
{
    val stdErrCleanSam : String = "stderr/CleanSam"
    val stdErrMarkDuplicates : String = "stderr/MarkDuplicates"
    val stdErrAddOrReplaceReadGroups : String = "stderr/AddOrReplaceReadGroups"
    val stdErrBuildBamIndex : String = "stderr/BuildBamIndex"
    val stdErrIntersect : String = "stderr/Intersect"
    val stdErrRealignerTargetCreator : String = "stderr/RealignerTargetCreator"
    val stdErrIndelRealigner : String = "stderr/IndelRealigner"
    val stdErrBaseRecalibrator : String = "stderr/BaseRecalibrator"
    val stdErrPrintReads : String = "stderr/PrintReads"
    val stdErrHaplotypeCaller : String = "stderr/HaplotypeCaller"
    val stdOutCleanSam : String = "stdout/CleanSam"
    val stdOutMarkDuplicates : String = "stdout/MarkDuplicates"
    val stdOutAddOrReplaceReadGroups : String = "stdout/AddOrReplaceReadGroups"
    val stdOutBuildBamIndex : String = "stdout/BuildBamIndex"
    val stdOutIntersect : String = "stdout/Intersect "
    val stdOutRealignerTargetCreator : String = "stdout/RealignerTargetCreator"
    val stdOutIndelRealigner : String = "stdout/IndelRealigner"
    val stdOutBaseRecalibrator : String = "stdout/BaseRecalibrator"
    val stdOutPrintReads : String = "stdout/PrintReads"
    val stdOutHaplotypeCaller : String = "stdout/HaplotypeCaller"

    val stdErrCleanSamFile = new File(stdErrCleanSam)
    val stdErrMarkDuplicatesFile = new File(stdErrMarkDuplicates)
    val stdErrAddOrReplaceReadGroupsFile = new File(stdErrAddOrReplaceReadGroups)
    val stdErrBuildBamIndexFile = new File(stdErrBuildBamIndex)
    val stdErrIntersectFile = new File(stdErrIntersect)
    val stdErrRealignerTargetCreatorFile = new File(stdErrRealignerTargetCreator)
    val stdErrIndelRealignerFile = new File(stdErrIndelRealigner)
    val stdErrBaseRecalibratorFile = new File(stdErrBaseRecalibrator)
    val stdErrPrintReadsFile = new File(stdErrPrintReads)
    val stdErrHaplotypeCallerFile = new File(stdErrHaplotypeCaller)
    val stdOutCleanSamFile = new File(stdOutCleanSam)
    val stdOutMarkDuplicatesFile = new File(stdOutMarkDuplicates)
    val stdOutAddOrReplaceReadGroupsFile = new File(stdOutAddOrReplaceReadGroups)
    val stdOutBuildBamIndexFile = new File(stdOutBuildBamIndex)
    val stdOutIntersectFile = new File(stdOutIntersect)
    val stdOutRealignerTargetCreatorFile = new File(stdOutRealignerTargetCreator)
    val stdOutIndelRealignerFile = new File(stdOutIndelRealigner)
    val stdOutBaseRecalibratorFile = new File(stdOutBaseRecalibrator)
    val stdOutPrintReadsFile = new File(stdOutPrintReads)
    val stdOutHaplotypeCallerFile = new File(stdOutHaplotypeCaller)

    
    if(stdErrCleanSamFile.exists() && stdErrCleanSamFile.isDirectory()){
        deleteFolder(stdErrCleanSamFile)
    }
    stdErrCleanSamFile.mkdirs


    if(stdErrMarkDuplicatesFile.exists() && stdErrMarkDuplicatesFile.isDirectory()){
        deleteFolder(stdErrMarkDuplicatesFile)
    }
    stdErrMarkDuplicatesFile.mkdirs


    if(stdErrAddOrReplaceReadGroupsFile.exists() && stdErrAddOrReplaceReadGroupsFile.isDirectory()){
        deleteFolder(stdErrAddOrReplaceReadGroupsFile)
    }
    stdErrAddOrReplaceReadGroupsFile.mkdirs


    if(stdErrBuildBamIndexFile.exists() && stdErrBuildBamIndexFile.isDirectory()){
        deleteFolder(stdErrBuildBamIndexFile)
    }
    stdErrBuildBamIndexFile.mkdirs


    if(stdErrIntersectFile.exists() && stdErrIntersectFile.isDirectory()){
        deleteFolder(stdErrIntersectFile)
    }
    stdErrIntersectFile.mkdirs


    if(stdErrRealignerTargetCreatorFile.exists() && stdErrRealignerTargetCreatorFile.isDirectory()){
        deleteFolder(stdErrRealignerTargetCreatorFile)
    }
    stdErrRealignerTargetCreatorFile.mkdirs


    if(stdErrIndelRealignerFile.exists() && stdErrIndelRealignerFile.isDirectory()){
        deleteFolder(stdErrIndelRealignerFile)
    }
    stdErrIndelRealignerFile.mkdirs


    if(stdErrBaseRecalibratorFile.exists() && stdErrBaseRecalibratorFile.isDirectory()){
        deleteFolder(stdErrBaseRecalibratorFile)
    }
    stdErrBaseRecalibratorFile.mkdirs


    if(stdErrPrintReadsFile.exists() && stdErrPrintReadsFile.isDirectory()){
        deleteFolder(stdErrPrintReadsFile)
    }
    stdErrPrintReadsFile.mkdirs


    if(stdErrHaplotypeCallerFile.exists() && stdErrHaplotypeCallerFile.isDirectory()){
        deleteFolder(stdErrHaplotypeCallerFile)
    }
    stdErrHaplotypeCallerFile.mkdirs


    if(stdOutCleanSamFile.exists() && stdOutCleanSamFile.isDirectory()){
        deleteFolder(stdOutCleanSamFile)
    }
    stdOutCleanSamFile.mkdirs


    if(stdOutMarkDuplicatesFile.exists() && stdOutMarkDuplicatesFile.isDirectory()){
        deleteFolder(stdOutMarkDuplicatesFile)
    }
    stdOutMarkDuplicatesFile.mkdirs


    if(stdOutAddOrReplaceReadGroupsFile.exists() && stdOutAddOrReplaceReadGroupsFile.isDirectory()){
        deleteFolder(stdOutAddOrReplaceReadGroupsFile)
    }
    stdOutAddOrReplaceReadGroupsFile.mkdirs


    if(stdOutBuildBamIndexFile.exists() && stdOutBuildBamIndexFile.isDirectory()){
        deleteFolder(stdOutBuildBamIndexFile)
    }
    stdOutBuildBamIndexFile.mkdirs


    if(stdOutIntersectFile.exists() && stdOutIntersectFile.isDirectory()){
        deleteFolder(stdOutIntersectFile)
    }
    stdOutIntersectFile.mkdirs


    if(stdOutRealignerTargetCreatorFile.exists() && stdOutRealignerTargetCreatorFile.isDirectory()){
        deleteFolder(stdOutRealignerTargetCreatorFile)
    }
    stdOutRealignerTargetCreatorFile.mkdirs


    if(stdOutIndelRealignerFile.exists() && stdOutIndelRealignerFile.isDirectory()){
        deleteFolder(stdOutIndelRealignerFile)
    }
    stdOutIndelRealignerFile.mkdirs


    if(stdOutBaseRecalibratorFile.exists() && stdOutBaseRecalibratorFile.isDirectory()){
        deleteFolder(stdOutBaseRecalibratorFile)
    }
    stdOutBaseRecalibratorFile.mkdirs


    if(stdOutPrintReadsFile.exists() && stdOutPrintReadsFile.isDirectory()){
        deleteFolder(stdOutPrintReadsFile)
    }
    stdOutPrintReadsFile.mkdirs


    if(stdOutHaplotypeCallerFile.exists() && stdOutHaplotypeCallerFile.isDirectory()){
        deleteFolder(stdOutHaplotypeCallerFile)
    }
    stdOutHaplotypeCallerFile.mkdirs


	val config = new Configuration()
	config.initialize("config.xml")

	val numInstances = Integer.parseInt(config.getNumInstances)
    val numRegions = Integer.parseInt(config.getNumRegions)
	val inputFolder = config.getInputFolder
	val outputFolder = config.getOutputFolder

	var mode = "local"

	val conf = new SparkConf().setAppName("DNASeqAnalyzer")
	// For local mode, include the following two lines
	if (mode == "local") {
	  conf.setMaster("local[" + config.getNumInstances() + "]")
	  conf.set("spark.cores.max", config.getNumInstances())
	}
	if (mode == "cluster") {
	  // For cluster mode, include the following commented line
	  conf.set("spark.shuffle.blockTransferService", "nio")
	}
	//conf.set("spark.rdd.compress", "true")

	new File(outputFolder).mkdirs
	new File(outputFolder + "output.vcf")
	val sc = new SparkContext(conf)
	val bcconfig = sc.broadcast(config)

    val bc_stdErrCleanSam = sc.broadcast(stdErrCleanSam)
    val bc_stdErrMarkDuplicates = sc.broadcast(stdErrMarkDuplicates)
    val bc_stdErrAddOrReplaceReadGroups = sc.broadcast(stdErrAddOrReplaceReadGroups)
    val bc_stdErrBuildBamIndex = sc.broadcast(stdErrBuildBamIndex)
    val bc_stdErrIntersect = sc.broadcast(stdErrIntersect)
    val bc_stdErrRealignerTargetCreator = sc.broadcast(stdErrRealignerTargetCreator)
    val bc_stdErrIndelRealigner = sc.broadcast(stdErrIndelRealigner)
    val bc_stdErrBaseRecalibrator = sc.broadcast(stdErrBaseRecalibrator)
    val bc_stdErrPrintReads = sc.broadcast(stdErrPrintReads)
    val bc_stdErrHaplotypeCaller = sc.broadcast(stdErrHaplotypeCaller)
    val bc_stdOutCleanSam = sc.broadcast(stdOutCleanSam)
    val bc_stdOutMarkDuplicates = sc.broadcast(stdOutMarkDuplicates)
    val bc_stdOutAddOrReplaceReadGroups = sc.broadcast(stdOutAddOrReplaceReadGroups)
    val bc_stdOutBuildBamIndex = sc.broadcast(stdOutBuildBamIndex)
    val bc_stdOutIntersect = sc.broadcast(stdOutIntersect)
    val bc_stdOutRealignerTargetCreator = sc.broadcast(stdOutRealignerTargetCreator)
    val bc_stdOutIndelRealigner = sc.broadcast(stdOutIndelRealigner)
    val bc_stdOutBaseRecalibrator = sc.broadcast(stdOutBaseRecalibrator)
    val bc_stdOutPrintReads = sc.broadcast(stdOutPrintReads)
    val bc_stdOutHaplotypeCaller = sc.broadcast(stdOutHaplotypeCaller)
	
	// Comment these two lines if you want to see more verbose messages from Spark
	Logger.getLogger("org").setLevel(Level.OFF);
	Logger.getLogger("akka").setLevel(Level.OFF);
	
	sc.addSparkListener(new SparkListener() 
	{
		override def onApplicationStart(applicationStart: SparkListenerApplicationStart) 
		{
			bw.write(getTimeStamp() + " Spark ApplicationStart: " + applicationStart.appName + "\n");
			bw.flush
		}

		override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) 
		{
			bw.write(getTimeStamp() + " Spark ApplicationEnd: " + applicationEnd.time + "\n");
			bw.flush
		}

		override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) 
		{
			val map = stageCompleted.stageInfo.rddInfos
			map.foreach(row => {
				if (row.isCached)
				{
					bw.write(getTimeStamp() + row.name + ": memsize = " + (row.memSize / 1000000) + "MB, rdd diskSize " + 
						row.diskSize + ", numPartitions = " + row.numPartitions + "-" + row.numCachedPartitions + "\n");
				}
				else if (row.name.contains("rdd_"))
				{
					bw.write(getTimeStamp() + row.name + " processed!\n");
				}
				bw.flush
			})
		}
	});
		
	var t0 = System.currentTimeMillis

	val files = sc.parallelize(new File(inputFolder).listFiles, numInstances)
	files.cache
	println("inputFolder = " + inputFolder + ", list of files = ")
	files.collect.foreach(x => println(x))
	
	var bwaResults = files.flatMap(files => samRead(files.getPath))
	  .combineByKey(
		(sam: SAMRecord) => Array(sam),
		(acc: Array[SAMRecord], value: SAMRecord) => (acc :+ value),
		(acc1: Array[SAMRecord], acc2: Array[SAMRecord]) => (acc1 ++ acc2)
	  ).persist(MEMORY_ONLY_SER)//cache
	bwaResults.setName("rdd_bwaResults")


    val vardesity_rdd = sc.textFile("output/vardensity.txt").map(line => line.split("\t")).map(array => (Integer.parseInt(array(3)),(array(0), Integer.parseInt(array(1)), Integer.parseInt(array(2)))))
    val vardesity = vardesity_rdd.collect

	var loadPerChromosome = bwaResults.map { case (key, values) => (values.length, key) }.collect
	val loadMap = loadBalancer(loadPerChromosome, vardesity, numRegions)
	
	val loadBalancedRdd = bwaResults.map {
	  case (key, values) =>
		(loadMap.indexWhere((a: ArrayBuffer[(String,Int,Int))]) => a.contains(key)), values)
	}.reduceByKey(_ ++ _)
	loadBalancedRdd.setName("rdd_loadBalancedRdd")

	val variantCallData = loadBalancedRdd
	  .flatMap { case (key: Int, sams: Array[SAMRecord]) => variantCall(key, sams, bcconfig, bc_stdErrCleanSam, bc_stdErrMarkDuplicates, bc_stdErrAddOrReplaceReadGroups, bc_stdErrBuildBamIndex, bc_stdErrIntersect, bc_stdErrRealignerTargetCreator, bc_stdErrIndelRealigner, bc_stdErrBaseRecalibrator, bc_stdErrPrintReads, bc_stdErrHaplotypeCaller,
      bc_stdOutCleanSam, bc_stdOutMarkDuplicates, bc_stdOutAddOrReplaceReadGroups, bc_stdOutBuildBamIndex, bc_stdOutIntersect, bc_stdOutRealignerTargetCreator, bc_stdOutIndelRealigner, bc_stdOutBaseRecalibrator, bc_stdOutPrintReads, bc_stdOutHaplotypeCaller) }
	variantCallData.setName("rdd_variantCallData")
	
	val results = variantCallData.combineByKey(
	  (value: (Int, String)) => Array(value),
	  (acc: Array[(Int, String)], value: (Int, String)) => (acc :+ value),
	  (acc1: Array[(Int, String)], acc2: Array[(Int, String)]) => (acc1 ++ acc2)
	).cache
	//results.setName("rdd_results")

	val fl = new PrintWriter(new File(outputFolder + "output.vcf"))
	for (i <- 1 to 24) {
	  println("Writing chrom: " + i.toString)
	  val fileDump = results.filter { case (chrom, value) => chrom == i }
		.flatMap { case (chrom: Int, value: Array[(Int, String)]) => value }
		.sortByKey(true)
		.map { case (position: Int, line: String) => line }
		.collect
	  for (line <- fileDump.toIterator) {
		fl.println(line)
	  }
	}
	fl.close()
	
	sc.stop()
	bw.close()
	
	val et = (System.currentTimeMillis - t0) / 1000 
	println(getTimeStamp() + "Execution time: %d mins %d secs".format(et/60, et%60))
}
  //////////////////////////////////////////////////////////////////////////////
} // End of Class definition
