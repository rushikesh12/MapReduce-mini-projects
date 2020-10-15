import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Graph {

    // For Job 1

    public static class MapperOne extends Mapper<Object, Text, LongWritable, LongWritable>{
        @Override
        public void map ( Object key, Text value, Context context )
                    throws IOException, InterruptedException{
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long x = s.nextLong();
            long y = s.nextLong();
            context.write(new LongWritable(x),new LongWritable(y));
            s.close();
        }
    }

    public static class ReducerOne extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable v: values) {
                count++;
            };
            context.write(key,new LongWritable (count));
        }
    }

    // For Job 2

    public static class MapperTwo extends Mapper<Object, Text, LongWritable, LongWritable>{
        @Override
        public void map ( Object key, Text value, Context context )
                    throws IOException, InterruptedException{
            Scanner f = new Scanner(value.toString()).useDelimiter("\t");
            long a = f.nextLong();
            long b = f.nextLong();
            long c = 1;
            context.write(new LongWritable(b), new LongWritable(c));
            f.close();
        }
    }

    public static class ReducerTwo extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable v: values) {
                sum += v.get();
            };
            context.write(key,new LongWritable (sum));
        }
    }

    public static void main ( String[] args ) throws Exception {
        // Job 1 
        Job job = Job.getInstance();
        job.setJobName("Job1");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(MapperOne.class);
        job.setReducerClass(ReducerOne.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path("./temp"));
        job.waitForCompletion(true);

        // Job 2
        Job job2 = Job.getInstance();
        job2.setJobName("Job2");
        job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setMapperClass(MapperTwo.class);
        job2.setReducerClass(ReducerTwo.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path("./temp"));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        job2.waitForCompletion(true);


   }
}
