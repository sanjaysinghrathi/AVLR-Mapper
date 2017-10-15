//package org.sparkexample;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.spark.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.storage.StorageLevel.*;
import com.google.common.base.Optional.*;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.SizeEstimator;
import scala.Int;
import scala.Tuple2;
import scala.Tuple3;
import scala.math.Ordering;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
//import org.sparkexample.DNAString.*;

public class AVLR_Mapper {



    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Please provide the input file full path as argument");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().set("textinputformat.record.delimiter","@").setAppName("org.sparkexample.WordCount");
        //Configuration conf1= new Configuration();
        //conf1.set("textinputformat.record.delimiter", args[1]);
        JavaSparkContext context = new JavaSparkContext(conf);

        long starttime=System.currentTimeMillis();

        JavaRDD<String> file22=context.textFile(args[1]);
        Broadcast<JavaRDD<String>> file22b=context.broadcast(file22);
        final int keylength=Integer.parseInt(args[3]);
        final int sizeofref=Integer.parseInt(args[2].toString());
        final PairFlatMapFunction<String,String,Long> WORDS_EXTRACTOR99b1A =
                new PairFlatMapFunction<String, String, Long>() {

                    public Iterable<Tuple2<String,Long>> call(String s) throws Exception {
                        String[] a=s.split(",");
                        //int g1= Long.parseInt(a[0]);
                        long g1=Long.valueOf(a[0]);
                        a[1]=a[1].toUpperCase();
                        //char[] array1=a[1].toCharArray();
                        int size =a[1].length();
                        List<Tuple2<String,Long>> result = new ArrayList<Tuple2<String,Long>>();
                        if(size>=(sizeofref+keylength-1)){
                            for(int i=0;i<sizeofref;i++){
                                if(a[1].charAt(i)=='A') {
                                    long g2 = i + g1;
                                    //
                                    String a2 = a[1].substring(i, i + keylength);
                                    result.add(new Tuple2<String, Long>(a2, g2));
                                }
                            }
                        }
                        else if(a[1].startsWith(">")){
                            int size1=size-sizeofref;
                            for(int j=0;j<size1;j++){
                                if(a[1].charAt(j)=='A') {
                                    long g2 = j + g1;
                                    String a2 = a[1].substring(j, size1);
                                    //String g3 = array1[j] + "," + g2;
                                    result.add(new Tuple2<String, Long>(a2, g2));
                                }
                            }

                        }
                        else if (size>=sizeofref){
                            for(int j=0;j<sizeofref;j++){
                                if(a[1].charAt(j)=='A') {
                                    long g2 = j + g1;
                                    //String g3 = array1[j] + "," + g2;
                                    if (j + keylength <= size) {
                                        String a2 = a[1].substring(j, j + keylength);

                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    } else {
                                        String a2 = a[1].substring(j, size);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    }
                                }
                            }
                        }
                        else {
                            for(int j=0;j<size;j++){
                                if(a[1].charAt(j)=='A') {
                                    long g2 = j + g1;
                                    //String g3 = array1[j] + "," + g2;
                                    if (j + keylength <= size) {
                                        String a2 = a[1].substring(j, j + keylength);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    } else {
                                        String a2 = a[1].substring(j, size);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    }
                                }
                            }
                        }

                        return result;
                    }
                };
        final PairFlatMapFunction<String,String,Long> WORDS_EXTRACTOR99b1C =
                new PairFlatMapFunction<String, String, Long>() {

                    public Iterable<Tuple2<String,Long>> call(String s) throws Exception {
                        String[] a=s.split(",");
                        //int g1= Long.parseInt(a[0]);
                        long g1=Long.valueOf(a[0]);
                        a[1]=a[1].toUpperCase();
                        //char[] array1=a[1].toCharArray();
                        int size =a[1].length();
                        List<Tuple2<String,Long>> result = new ArrayList<Tuple2<String,Long>>();
                        if(size>=(sizeofref+keylength-1)){
                            for(int i=0;i<sizeofref;i++){
                                if(a[1].charAt(i)=='C') {
                                    long g2 = i + g1;
                                    //
                                    String a2 = a[1].substring(i, i + keylength);
                                    result.add(new Tuple2<String, Long>(a2, g2));
                                }
                            }
                        }
                        else if(a[1].startsWith(">")){
                            int size1=size-sizeofref;
                            for(int j=0;j<size1;j++){
                                if(a[1].charAt(j)=='C') {
                                    long g2 = j + g1;
                                    String a2 = a[1].substring(j, size1);
                                    //String g3 = array1[j] + "," + g2;
                                    result.add(new Tuple2<String, Long>(a2, g2));
                                }
                            }

                        }
                        else if (size>=sizeofref){
                            for(int j=0;j<sizeofref;j++){
                                if(a[1].charAt(j)=='C') {
                                    long g2 = j + g1;
                                    //String g3 = array1[j] + "," + g2;
                                    if (j + keylength <= size) {
                                        String a2 = a[1].substring(j, j + keylength);

                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    } else {
                                        String a2 = a[1].substring(j, size);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    }
                                }
                            }
                        }
                        else {
                            for(int j=0;j<size;j++){
                                if(a[1].charAt(j)=='C') {
                                    long g2 = j + g1;
                                    //String g3 = array1[j] + "," + g2;
                                    if (j + keylength <= size) {
                                        String a2 = a[1].substring(j, j + keylength);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    } else {
                                        String a2 = a[1].substring(j, size);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    }
                                }
                            }
                        }

                        return result;
                    }
                };
        final PairFlatMapFunction<String,String,Long> WORDS_EXTRACTOR99b1G =
                new PairFlatMapFunction<String, String, Long>() {

                    public Iterable<Tuple2<String,Long>> call(String s) throws Exception {
                        String[] a=s.split(",");
                        //int g1= Long.parseInt(a[0]);
                        long g1=Long.valueOf(a[0]);
                        a[1]=a[1].toUpperCase();
                        //char[] array1=a[1].toCharArray();
                        int size =a[1].length();
                        List<Tuple2<String,Long>> result = new ArrayList<Tuple2<String,Long>>();
                        if(size>=(sizeofref+keylength-1)){
                            for(int i=0;i<sizeofref;i++){
                                if(a[1].charAt(i)=='G') {
                                    long g2 = i + g1;
                                    //
                                    String a2 = a[1].substring(i, i + keylength);
                                    result.add(new Tuple2<String, Long>(a2, g2));
                                }
                            }
                        }
                        else if(a[1].startsWith(">")){
                            int size1=size-sizeofref;
                            for(int j=0;j<size1;j++){
                                if(a[1].charAt(j)=='G') {
                                    long g2 = j + g1;
                                    String a2 = a[1].substring(j, size1);
                                    //String g3 = array1[j] + "," + g2;
                                    result.add(new Tuple2<String, Long>(a2, g2));
                                }
                            }

                        }
                        else if (size>=sizeofref){
                            for(int j=0;j<sizeofref;j++){
                                if(a[1].charAt(j)=='G') {
                                    long g2 = j + g1;
                                    //String g3 = array1[j] + "," + g2;
                                    if (j + keylength <= size) {
                                        String a2 = a[1].substring(j, j + keylength);

                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    } else {
                                        String a2 = a[1].substring(j, size);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    }
                                }
                            }
                        }
                        else {
                            for(int j=0;j<size;j++){
                                if(a[1].charAt(j)=='G') {
                                    long g2 = j + g1;
                                    //String g3 = array1[j] + "," + g2;
                                    if (j + keylength <= size) {
                                        String a2 = a[1].substring(j, j + keylength);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    } else {
                                        String a2 = a[1].substring(j, size);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    }
                                }
                            }
                        }

                        return result;
                    }
                };
        final PairFlatMapFunction<String,String,Long> WORDS_EXTRACTOR99b1N =
                new PairFlatMapFunction<String, String, Long>() {

                    public Iterable<Tuple2<String,Long>> call(String s) throws Exception {
                        String[] a=s.split(",");
                        //int g1= Long.parseInt(a[0]);
                        long g1=Long.valueOf(a[0]);
                        a[1]=a[1].toUpperCase();
                        //char[] array1=a[1].toCharArray();
                        int size =a[1].length();
                        List<Tuple2<String,Long>> result = new ArrayList<Tuple2<String,Long>>();
                        if(size>=(sizeofref+keylength-1)){
                            for(int i=0;i<sizeofref;i++){
                                if(a[1].charAt(i)=='N') {
                                    long g2 = i + g1;
                                    //
                                    String a2 = a[1].substring(i, i + keylength);
                                    result.add(new Tuple2<String, Long>(a2, g2));
                                }
                            }
                        }
                        else if(a[1].startsWith(">")){
                            int size1=size-sizeofref;
                            for(int j=0;j<size1;j++){
                                if(a[1].charAt(j)=='N') {
                                    long g2 = j + g1;
                                    String a2 = a[1].substring(j, size1);
                                    //String g3 = array1[j] + "," + g2;
                                    result.add(new Tuple2<String, Long>(a2, g2));
                                }
                            }

                        }
                        else if (size>=sizeofref){
                            for(int j=0;j<sizeofref;j++){
                                if(a[1].charAt(j)=='N') {
                                    long g2 = j + g1;
                                    //String g3 = array1[j] + "," + g2;
                                    if (j + keylength <= size) {
                                        String a2 = a[1].substring(j, j + keylength);

                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    } else {
                                        String a2 = a[1].substring(j, size);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    }
                                }
                            }
                        }
                        else {
                            for(int j=0;j<size;j++){
                                if(a[1].charAt(j)=='N') {
                                    long g2 = j + g1;
                                    //String g3 = array1[j] + "," + g2;
                                    if (j + keylength <= size) {
                                        String a2 = a[1].substring(j, j + keylength);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    } else {
                                        String a2 = a[1].substring(j, size);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    }
                                }
                            }
                        }

                        return result;
                    }
                };
        final PairFlatMapFunction<String,String,Long> WORDS_EXTRACTOR99b1T =
                new PairFlatMapFunction<String, String, Long>() {

                    public Iterable<Tuple2<String,Long>> call(String s) throws Exception {
                        String[] a=s.split(",");
                        //int g1= Long.parseInt(a[0]);
                        long g1=Long.valueOf(a[0]);
                        a[1]=a[1].toUpperCase();
                        //char[] array1=a[1].toCharArray();
                        int size =a[1].length();
                        List<Tuple2<String,Long>> result = new ArrayList<Tuple2<String,Long>>();
                        if(size>=(sizeofref+keylength-1)){
                            for(int i=0;i<sizeofref;i++){
                                if(a[1].charAt(i)=='T') {
                                    long g2 = i + g1;
                                    //
                                    String a2 = a[1].substring(i, i + keylength);
                                    result.add(new Tuple2<String, Long>(a2, g2));
                                }
                            }
                        }
                        else if(a[1].startsWith(">")){
                            int size1=size-sizeofref;
                            for(int j=0;j<size1;j++){
                                if(a[1].charAt(j)=='T') {
                                    long g2 = j + g1;
                                    String a2 = a[1].substring(j, size1);
                                    //String g3 = array1[j] + "," + g2;
                                    result.add(new Tuple2<String, Long>(a2, g2));
                                }
                            }

                        }
                        else if (size>=sizeofref){
                            for(int j=0;j<sizeofref;j++){
                                if(a[1].charAt(j)=='T') {
                                    long g2 = j + g1;
                                    //String g3 = array1[j] + "," + g2;
                                    if (j + keylength <= size) {
                                        String a2 = a[1].substring(j, j + keylength);

                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    } else {
                                        String a2 = a[1].substring(j, size);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    }
                                }
                            }
                        }
                        else {
                            for(int j=0;j<size;j++){
                                if(a[1].charAt(j)=='T') {
                                    long g2 = j + g1;
                                    //String g3 = array1[j] + "," + g2;
                                    if (j + keylength <= size) {
                                        String a2 = a[1].substring(j, j + keylength);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    } else {
                                        String a2 = a[1].substring(j, size);
                                        result.add(new Tuple2<String, Long>(a2, g2));
                                    }
                                }
                            }
                        }

                        return result;
                    }
                };





        final PairFlatMapFunction<Tuple2<String,Long>,Integer,String> WORDS_EXTRACTOR99b2g =
                new PairFlatMapFunction<Tuple2<String,Long>, Integer,String>() {

                    public Iterable<Tuple2<Integer,String>> call(Tuple2<String,Long> s) throws Exception {

                        int g=Integer.parseInt(s._2().toString())+1;
                        List<Tuple2<Integer, String>> result = new ArrayList<Tuple2<Integer, String>>();
                        String[] st=s._1().split(",");
                        int g2=Integer.parseInt(st[1]);
                        result.add(new Tuple2<Integer, String>(g,st[1]));
                        result.add(new Tuple2<Integer, String>(g2,st[0]));
                        return result;
                    }
                };
        final Function2<String, String,String> WORDS_EXTRACTOR99b3 =
                new Function2<String, String,String>() {

                    public String call(String t,String s) throws Exception {
                        String g=new String();
                        if(t.length()>1){
                            g=s+","+t;
                        }
                        else if(s.length()>1){
                            g=t+","+s;
                        }
                        else{
                            if(Character.isDigit(t.charAt(0))){
                                g=s+","+t;
                            }
                            else{
                                g=t+","+s;
                            }
                        }
                        return g;
                    }
                };
        final Function<Tuple2<Integer,String>,String> WORDS_EXTRACTOR99b4 =
                new Function<Tuple2<Integer,String>,String>() {

                    public String call(Tuple2<Integer,String> s) throws Exception {
                        String g=new String();
                        //if(s._1().length()>1){
                        //g=s._2()+s._1();
                        //}
                        //else{
                        //if(Character.is)
                        //}
                        return s._2()+","+s._1();
                    }
                };

        //file2.flatMapToPair(WORDS_EXTRACTOR99b1).sortByKey().values().saveAsTextFile(args[6]);*/
        //file22b.value().flatMapToPair(WORDS_EXTRACTOR99b1A).sortByKey().values().saveAsTextFile(args[5]);
        //file22b.value().flatMapToPair(WORDS_EXTRACTOR99b1C).sortByKey().values().saveAsTextFile(args[6]);
        file22b.value().flatMapToPair(WORDS_EXTRACTOR99b1G).sortByKey().values().saveAsTextFile(args[7]);
        //file22.flatMapToPair(WORDS_EXTRACTOR99b1N).sortByKey().values().saveAsTextFile(args[8]);
        //file22b.value().flatMapToPair(WORDS_EXTRACTOR99b1T).sortByKey().values().saveAsTextFile(args[9]);

        /**JavaRDD<Long> suffixa=file22b.value().flatMapToPair(WORDS_EXTRACTOR99b1A).sortByKey().values();
         suffixa.persist(StorageLevel.MEMORY_AND_DISK_SER());
         suffixa.saveAsTextFile(args[5]);
         suffixa.unpersist();

         JavaRDD<Long> suffixc=file22b.value().flatMapToPair(WORDS_EXTRACTOR99b1C).sortByKey().values();
         suffixc.persist(StorageLevel.MEMORY_AND_DISK_SER());
         suffixc.saveAsTextFile(args[6]);
         suffixc.unpersist();

         JavaRDD<Long> suffixg=file22b.value().flatMapToPair(WORDS_EXTRACTOR99b1G).sortByKey().values();
         suffixg.persist(StorageLevel.MEMORY_AND_DISK_SER());
         suffixg.saveAsTextFile(args[7]);
         suffixg.unpersist();

         //JavaRDD<Long> suffixn=file22.flatMapToPair(WORDS_EXTRACTOR99b1N).sortByKey().values();
         //suffixn.saveAsTextFile(args[15]);
         //suffixn.unpersist();

         JavaRDD<Long> suffixt=file22b.value().flatMapToPair(WORDS_EXTRACTOR99b1T).sortByKey().values();
         suffixt.persist(StorageLevel.MEMORY_AND_DISK_SER());
         suffixt.saveAsTextFile(args[9]);
         //suffixt.unpersist();
         //List<String> ref1=filejoin2.collect();

         //for(String aa:ref1){
         //System.out.println("join1-------------------"+aa);
         //}
         */
        long starttime4=System.currentTimeMillis();
        System.out.println("total time taken is for creating suffix of G-------------------:: "+(starttime4-starttime));




        /**final PairFlatMapFunction<Tuple2<Tuple2<LongWritable,Text>,Long>, Integer,Character> WORDS_EXTRACTOR99b =
         new PairFlatMapFunction<Tuple2<Tuple2<LongWritable,Text>,Long>, Integer,Character>() {

         public Iterable<Tuple2<Integer,Character>> call(Tuple2<Tuple2<LongWritable,Text>,Long> s) throws Exception {
         int g1=Integer.parseInt(s._1()._1().toString());
         int g3=Integer.parseInt(s._2().toString());
         int g2=g1-g3+1;
         List<Tuple2<Integer, Character>> result = new ArrayList<Tuple2<Integer, Character>>();

         char[] abc= s._1()._2().toString().toUpperCase().toCharArray();
         String aa=new String();
         int jj=abc.length;
         for(int i=0; i<jj;i++){
         int ii=i+g2;
         //aa=aa+abc[i]+"z"+ii+"p";
         result.add(new Tuple2<Integer, Character>(ii,abc[i]));

         }
         return result;
         }
         };
         final Function<Tuple2<Tuple2<LongWritable,Text>,Long>, Boolean> WORDS_EXTRACTOR3 =
         new Function<Tuple2<Tuple2<LongWritable,Text>,Long>, Boolean>() {
         public Boolean call(Tuple2<Tuple2<LongWritable,Text>,Long> T) throws Exception {
         Boolean a=false;
         Long max=T._1()._1().get()-T._2().longValue()+1;
         if(max<=maxarraysize){
         a=true;
         }
         return a;
         }
         };

         //System.out.println("read file size is--------------------"+file1.count());
         //JavaPairRDD<Tuple2<LongWritable,Text>,Long> file10=file1.zipWithIndex();
         //file10.persist(StorageLevel.MEMORY_AND_DISK_SER());
         //System.out.println("read file11 size is--------------------"+file10.count());
         JavaPairRDD<Integer, Character> file11g=file11.value().zipWithIndex().filter(WORDS_EXTRACTOR3).flatMapToPair(WORDS_EXTRACTOR99b);
         file11g.persist(StorageLevel.MEMORY_AND_DISK_SER());
         //System.out.println("read file11 size is--------------------"+file11.count());
         JavaPairRDD<Integer, Character> file12=file11g.sortByKey();
         file12.persist(StorageLevel.MEMORY_AND_DISK_SER());
         //System.out.println("read file12 size is--------------------"+file12.count());
         //JavaRDD<Character> file13=file12.values();
         //file13.persist(StorageLevel.MEMORY_AND_DISK_SER());

         file12.values().saveAsTextFile(args[4]);
         //System.out.println("read file13 size is--------------------"+file13.count());
         //final List<Character> data1=file13.collect();

         //final int length1=data1.size();
         //System.out.println("size of file1--------------------"+length1);

         //final Broadcast<List<Character>> data1b=context.broadcast(data1);
         //JavaRDD<Integer> file14=file12.keys();
         //file14.persist(StorageLevel.MEMORY_AND_DISK_SER());

         //Broadcast<JavaRDD<Integer>> file14b=context.broadcast(file14);
         //System.out.println("read file14 size is--------------------"+file14b.value().count());

         file11g.unpersist();
         file12.unpersist();

         //file12.unpersist();
         //file13.unpersist();
         //file14.unpersist();
         //file11.unpersist();

         //final List<Character> data1=file11.value().zipWithIndex().filter(WORDS_EXTRACTOR3).flatMapToPair(WORDS_EXTRACTOR99b).sortByKey().values().collect();
         //final int length1=data1.size();
         //System.out.println("size of file1--------------------"+length1);
         //for(char aa:data1){
         //System.out.println("ref items are--------------------"+aa);
         //}

         // suffix for 1st file





         final PairFlatMapFunction<Tuple2<Tuple2<LongWritable,Text>,Long>, Integer,Character> WORDS_EXTRACTOR99c =
         new PairFlatMapFunction<Tuple2<Tuple2<LongWritable,Text>,Long>, Integer,Character>() {

         public Iterable<Tuple2<Integer,Character>> call(Tuple2<Tuple2<LongWritable,Text>,Long> s) throws Exception {
         Long g1=s._1()._1().get();
         Long g3=s._2();
         Long g2=g1-g3+1;
         int g4=(int) (g2-maxarraysize);

         List<Tuple2<Integer, Character>> result = new ArrayList<Tuple2<Integer, Character>>();

         char[] abc= s._1()._2().toString().toUpperCase().toCharArray();
         String aa=new String();
         int jj=abc.length;
         for(int i=0; i<jj;i++){
         int ii=i+g4;
         //aa=aa+abc[i]+"z"+ii+"p";
         result.add(new Tuple2<Integer, Character>(ii,abc[i]));

         }
         return result;
         }
         };
         final Function<Tuple2<Tuple2<LongWritable,Text>,Long>, Boolean> WORDS_EXTRACTOR4 =
         new Function<Tuple2<Tuple2<LongWritable,Text>,Long>, Boolean>() {
         public Boolean call(Tuple2<Tuple2<LongWritable,Text>,Long> T) throws Exception {
         Boolean a=false;
         Long max=T._1()._1().get()-T._2().longValue()+1;
         if(max>maxarraysize){
         a=true;
         }
         return a;
         }
         };

         //final List<Character> ref1=file1.zipWithIndex().filter(WORDS_EXTRACTOR4).flatMapToPair(WORDS_EXTRACTOR99c).sortByKey().values().collect();
         //List<Tuple2<Integer,Character>> ref1=line555gggg.collect();
         //final int length2=ref1.size();
         //System.out.println("size of file2--------------------"+length2);
         //for(char aa:ref1){
         //System.out.println("ref items are--------------------"+aa);
         //}


         JavaPairRDD<Integer, Character> file21g=file11.value().zipWithIndex().filter(WORDS_EXTRACTOR4).flatMapToPair(WORDS_EXTRACTOR99c);
         file21g.persist(StorageLevel.MEMORY_AND_DISK_SER());
         //System.out.println("read file11 size is--------------------"+file11.count());
         JavaPairRDD<Integer, Character> file22=file21g.sortByKey();
         file22.persist(StorageLevel.MEMORY_AND_DISK_SER());
         //System.out.println("read file12 size is--------------------"+file12.count());

         //JavaRDD<Character> file23=file22.values();
         //file23.persist(StorageLevel.MEMORY_AND_DISK_SER());

         file22.values().saveAsTextFile(args[5]);
         //System.out.println("read file13 size is--------------------"+file13.count());
         //final List<Character> data2=file23.collect();

         //final int length2=data2.size();
         //System.out.println("size of file2--------------------"+length2);

         //final Broadcast<List<Character>> data2b=context.broadcast(data2);
         //JavaRDD<Integer> file14=file12.keys();
         //file14.persist(StorageLevel.MEMORY_AND_DISK_SER());

         //Broadcast<JavaRDD<Integer>> file14b=context.broadcast(file14);
         //System.out.println("read file14 size is--------------------"+file14b.value().count());

         file21g.unpersist();
         file22.unpersist();

         //file12.unpersist();
         //file23.unpersist();


         */


        context.close();
    }
}
