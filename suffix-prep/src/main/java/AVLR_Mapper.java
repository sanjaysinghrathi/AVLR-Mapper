
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

        //JavaPairRDD<LongWritable,Text> file2=context.newAPIHadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class, conf1);
        long starttime=System.currentTimeMillis();
        Configuration conf2= new Configuration();
        conf2.set("textinputformat.record.delimiter", "\n");
        JavaPairRDD<LongWritable,Text> file1=context.newAPIHadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class, conf2);
        //JavaRDD<String> file5=context.textFile(args[0]);
        //long sizeref=file1.count();
        Broadcast<JavaPairRDD<LongWritable,Text>> file11=context.broadcast(file1);
        //System.out.println("size of broadcast--------------------"+file11.value().count()+"-- last line");
        long starttime0=System.currentTimeMillis();

        final Function<Tuple2<LongWritable,Text>, Long> WORDS_EXTRACTORchrindex =
                new Function<Tuple2<LongWritable,Text>, Long>() {

                    public Long call(Tuple2<LongWritable,Text> s) throws Exception {
                        long g1=s._1().get();
                        //long g3=s._2();
                        //long g2=g1-2*g3;
                        //List<Tuple2<Integer, Character>> result = new ArrayList<Tuple2<Integer, Character>>();

                        String abc= s._2().toString();
                        //String aa=new String();
                        //int jj=abc.length;
                        //int length=(int) abc.length();
                        long g4=g1+abc.length()+1;
                        return g4;
                    }
                };


        final List<Long> chrindex1=file11.value().map(WORDS_EXTRACTORchrindex).collect();
        //List<Tuple2<Long,String>> chrindex1=chrindex.collect();
        long sizeref=file11.value().count();
        //long lastref=file11.value().keys().max(LongWritable.Comparator);
        final int indexsize=chrindex1.size();
        long finalsize=chrindex1.get(indexsize-1)-sizeref;
        //chrindex1.clear();
        long starttime10=System.currentTimeMillis();
        System.out.println("size of reference genome is--------------------::"+finalsize+"--time taken is -"+(starttime10-starttime0));

        final PairFunction<Tuple2<Tuple2<LongWritable,Text>,Long>, Long,String> WORDS_EXTRACTORchrindex2 =
                new PairFunction<Tuple2<Tuple2<LongWritable,Text>,Long>, Long,String>() {

                    public Tuple2<Long,String> call(Tuple2<Tuple2<LongWritable,Text>,Long> s) throws Exception {
                        long g1=s._1()._1().get();
                        long g3=s._2();
                        long g2=g1-g3+1;
                        //List<Tuple2<Integer, Character>> result = new ArrayList<Tuple2<Integer, Character>>();

                        String abc= s._1()._2().toString();
                        String aa=new String();
                        //int jj=abc.length;
                        if(abc.contains(">")){
                            //char[] gg=abc.toCharArray();
                            int size=abc.length();
                            long g4=g2+size;
                            aa=abc.substring(1, size)+","+g4;
                        }
                        return new Tuple2<Long, String>(g2, aa);
                    }
                };

        final Function<Tuple2<Tuple2<LongWritable,Text>,Long>, Boolean> WORDS_EXTRACTOR2gg =
                new Function<Tuple2<Tuple2<LongWritable,Text>,Long>, Boolean>() {
                    public Boolean call(Tuple2<Tuple2<LongWritable,Text>,Long> T) throws Exception {
                        Boolean a=false;
                        if(T._1()._2().toString().startsWith(">")){
                            a=true;
                        }
                        return a;
                    }
                };

        file11.value().zipWithIndex().filter(WORDS_EXTRACTOR2gg).mapToPair(WORDS_EXTRACTORchrindex2).values().saveAsTextFile(args[4]);

        final int sizeofref=Integer.parseInt(args[2].toString());



        final PairFunction<Tuple2<Tuple2<LongWritable,Text>,Long>, Integer,String> WORDS_EXTRACTOR99b1 =
                new PairFunction<Tuple2<Tuple2<LongWritable,Text>,Long>, Integer,String>() {

                    public Tuple2<Integer,String> call(Tuple2<Tuple2<LongWritable,Text>,Long> s) throws Exception {
                        //int g1=Integer.parseInt(s._1()._1().toString());
                        int g3=Integer.parseInt(s._2().toString());
                        //int g2=g1-g3+1;
                        long g1=s._1()._1().get();
                        //long g3=s._2();
                        long g2=g1-g3+1;
                        //long g=s._2();
                        //int g4=(int) g;
                        String a=g2+","+s._1()._2().toString();
                        return new Tuple2<Integer, String>(g3,a);
                    }
                };

        //System.out.println("read file size is--------------------"+file1.count());
        //JavaPairRDD<Tuple2<LongWritable,Text>,Long> file10=file1.zipWithIndex();
        //file10.persist(StorageLevel.MEMORY_AND_DISK_SER());
        //System.out.println("read file11 size is--------------------"+file10.count());
        JavaPairRDD<Integer, String> filejoin1=file11.value().zipWithIndex().mapToPair(WORDS_EXTRACTOR99b1);
        //filejoin1.persist(StorageLevel.MEMORY_AND_DISK_SER());
        final long join1length=sizeref;
        //List<Tuple2<Integer,String>> ref1=filejoin1.collect();
        //final int length2=ref1.size();
        //System.out.println("size of file1--------------------"+join1length);

        //long starttime1=System.currentTimeMillis();
        //System.out.println("file 1 creation time is--------------------"+(starttime1-starttime));
        //for(Tuple2<Integer,String> aa:ref1){
        //System.out.println("join1-------------------"+aa);
        //}

        final PairFunction<Tuple2<Tuple2<LongWritable,Text>,Long>, Integer,String> WORDS_EXTRACTOR99b2 =
                new PairFunction<Tuple2<Tuple2<LongWritable,Text>,Long>, Integer,String>() {

                    public Tuple2<Integer,String> call(Tuple2<Tuple2<LongWritable,Text>,Long> s) throws Exception {
                        long g=s._2()-1;
                        int g1=0;
                        String a=new String();
                        if(g>=0){
                            g1=(int) g;
                            a=s._1()._2().toString();
                        }
                        else{
                            g1=(int) join1length-1;
                            a=">";
                        }

                        return new Tuple2<Integer, String>(g1,a);
                    }
                };
        JavaPairRDD<Integer, String> filejoin2=file11.value().zipWithIndex().mapToPair(WORDS_EXTRACTOR99b2);
        //filejoin2.persist(StorageLevel.MEMORY_AND_DISK_SER());
        //List<Tuple2<Integer,String>> ref2=filejoin2.collect();
        //final int length2=ref1.size();
        //System.out.println("size of file2--------------------"+filejoin2.count());
        //long starttime2=System.currentTimeMillis();
        //System.out.println("file 2 creation time is--------------------"+(starttime2-starttime1));
        //for(Tuple2<Integer,String> aa:ref2){
        //System.out.println("join2-------------------"+aa);
        //}


        final PairFunction<Tuple2<Integer, Tuple2<String, String>>, Integer,String> WORDS_EXTRACTOR2 =
                new PairFunction<Tuple2<Integer, Tuple2<String, String>>, Integer,String>() {

                    public Tuple2<Integer,String> call(Tuple2<Integer, Tuple2<String, String>> s) throws Exception {
                        int g1=s._1();
                        String a=new String();
                        if(s._2()._2().contains(">")){
                            a=s._2()._1();
                        }
                        else{
                            a=s._2()._1()+s._2()._2();
                        }

                        return new Tuple2<Integer, String>(g1,a);
                    }
                };


        filejoin1.join(filejoin2).mapToPair(WORDS_EXTRACTOR2).sortByKey().values().saveAsTextFile(args[1]);
        //JavaRDD<String> file22=filejoin1.join(filejoin2).mapToPair(WORDS_EXTRACTOR2).sortByKey().values();
        //file22.persist(StorageLevel.MEMORY_AND_DISK_SER());
        //Broadcast<JavaRDD<String>> file22b=context.broadcast(file22);
        //List<Tuple2<Integer,String>> ref2=filejoin2.collect();
        //final int length2=ref1.size();
        //System.out.println("size of file22--------------------"+file22b.value().count());
        long starttime4=System.currentTimeMillis();
        System.out.println("total time taken is -------------------"+(starttime4-starttime)+"  size of reference genome is -"+finalsize);


        context.close();
    }
}
