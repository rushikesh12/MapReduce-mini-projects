import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;

import org.apache.hadoop.mapreduce.lib.output.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import java.lang.Integer;
import java.io.*;

class Elem implements Writable {
    public short tag;  
    public int index;  
    public double value;
    
    Elem () {}

    Elem (short tag, int index, double value){
        this.tag = tag;
        this.index = index;
        this.value = value;
    }
    @Override
    public void write (DataOutput out) throws IOException {
        out.writeShort(this.tag);
        out.writeInt(this.index);
        out.writeDouble(this.value);
    }
    @Override
    public void readFields (DataInput in) throws IOException {
        this.tag = in.readShort();
        this.index = in.readInt();
        this.value = in.readDouble();
    }
    
    public String toString () { 
        return String.valueOf(tag)+"\t"+String.valueOf(index)+"\t"+String.valueOf(value); 
    }
  }


class Pair implements WritableComparable <Pair> {
    public int i;
    public int j;

    Pair () {}

    Pair (int i, int j){
        this.i = i;
        this.j = j;
    }

    public void write ( DataOutput out ) throws IOException {
        out.writeInt(this.i);
        out.writeInt(this.j);
    }

    public void readFields ( DataInput in ) throws IOException {
        this.i = in.readInt();
        this.j = in.readInt();
        
    }

    public int compareTo ( Pair p){
        int cmp = Integer.compare(i, p.i);
        if (cmp != 0){
            return cmp;
        }
        cmp = Integer.compare(j, p.j);
        return cmp;

    }

    public String toString () { 
        return String.valueOf(i)+"\t"+String.valueOf(j); 
    }

}

public class Multiply {

    public static class MatrixOneMapper extends Mapper<Object,Text,IntWritable,Elem > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            short tag = 0;
            Elem e = new Elem (tag, i, v);
            context.write(new IntWritable(j), e);
            s.close();
        }
    }

    

    public static class MatrixTwoMapper extends Mapper<Object,Text,IntWritable,Elem > {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            short tag = 1;
            Elem e = new Elem (tag, j, v);
            context.write(new IntWritable(i), e);
            s.close();
        }
    }

    public static class ReducerOne extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
        ArrayList<Elem> A = new ArrayList<Elem>();
        ArrayList<Elem> B = new ArrayList<Elem>();
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {
            A.clear();
            B.clear();
            for (Elem v: values){
                if (v.tag == 0){
                    A.add(new Elem(v.tag, v.index, v.value));
                }                  
                else if (v.tag == 1) {
                    B.add(new Elem(v.tag, v.index, v.value));
                }
            };
                 
            // System.out.println("Vector A: "+A);
            // System.out.println("Vector B: "+B);
            for ( Elem a: A )
                for ( Elem b: B )
                    context.write(new Pair (a.index, b.index),new DoubleWritable(a.value * b.value));
        }
    }

    public static class MapperTwo extends Mapper<Pair, DoubleWritable, Pair, DoubleWritable>{
        @Override
        public void map ( Pair key, DoubleWritable value, Context context )
                    throws IOException, InterruptedException{
            
            
            context.write(key, value);
            
        }
    }

    public static class ReducerTwo extends Reducer<Pair,DoubleWritable,Text,DoubleWritable> {
        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            double m = 0;
            for (DoubleWritable v: values) {
                m = m + v.get();
            };
            // System.out.println(m);
            context.write(new Text (key.toString()),new DoubleWritable (m));
        }
    }

    public static void main ( String[] args ) throws Exception {
        // Job 1
        Job job1 = Job.getInstance();
        job1.setJobName("Job1");
        job1.setJarByClass(Multiply.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        job1.setReducerClass(ReducerOne.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MatrixOneMapper.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,MatrixTwoMapper.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);
        

        // Job 2
        Job job2 = Job.getInstance();
        job2.setJobName("Job2");
        job2.setJarByClass(Multiply.class);
        job2.setMapperClass(MapperTwo.class);
        job2.setReducerClass(ReducerTwo.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);
    }
}
