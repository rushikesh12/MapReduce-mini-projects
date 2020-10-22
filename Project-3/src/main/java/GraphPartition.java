import java.io.*;
import java.util.Scanner;
import java.util.Vector;
import java.lang.Object;
// import org.apache.mahout.math.VectorWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                                        // the vertex ID
    public Vector<Long> adjacent = new Vector<Long>();     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    /* ... */

    Vertex () {}
    
    public Vertex (long id1, Vector<Long> adjacent1, long centroid1, short depth1){
        adjacent = new Vector<>();
        this.id = id1;
        this.adjacent = adjacent1;
        this.centroid = centroid1;
        this.depth = depth1;
    }

    @Override
    public void write (DataOutput out) throws IOException {
        
        out.writeLong(id);
        out.writeLong(adjacent.size());
        for(int i=0;i<adjacent.size();i++)
            out.writeLong(adjacent.get(i));
        out.writeLong(centroid);
        out.writeShort(depth);
    }

    @Override
    public void readFields (DataInput in) throws IOException {
        adjacent = new Vector<Long>(); 
        this.id = in.readLong();
        long vector_size = in.readLong();
        for(int i=0; i<vector_size; i++)
            this.adjacent.addElement(in.readLong());
        // this.adjacent = temp;
        this.centroid = in.readLong();
        this.depth = in.readShort();

    }
    
    
}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
    static int count = 0;
    /* ... */
    public static class JobOneMapperOne extends Mapper<Object,Text,LongWritable,Vertex> {
        
        @Override
        public void map (Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            // adjacent.clear();
            
        
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            long id = s.nextLong(); 
            Vector<Long> adjacent = new Vector<Long>();
            while (s.hasNext()){
                adjacent.add(s.nextLong());
            }
            long centroid;
            if (count < 10){
                centroid = id;
                count++;
                centroids.add(id);
                
                context.write(new LongWritable(id), new Vertex(id,adjacent,centroid,BFS_depth));
            }else{
                centroid = -1;
                
                short depth = 0;
                context.write(new LongWritable(id), new Vertex(id,adjacent,centroid,BFS_depth));
            }
            
            // System.out.println("Adjacent vector:"+adjacent);
            // System.out.println("ID:"+line_number);
            // Vertex v = new Vertex(id,adjacent,centroid,depth);
            // System.out.println(String.valueOf(id) + "\t" +v.toString());
            // context.write(new LongWritable(id), new Vertex(id,adjacent,centroid,depth));
            s.close();
        }
    }
    public static class JobTwoMapperOne extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                        throws IOException, InterruptedException {
            Vector <Long> temp = new Vector <Long>(); 
            context.write(new LongWritable(value.id), value);
            if (value.centroid > 0){
                for (long n: value.adjacent){
                    context.write(new LongWritable(n), new Vertex(n,temp, value.centroid, BFS_depth));
                }
            }
        }
    }
    public static class JobTwoReducerOne extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {
        @Override
        public void reduce ( LongWritable key, Iterable<Vertex> values, Context context )
                           throws IOException, InterruptedException {
            short min_depth = 1000;
            long centroid = -1;
            short depth = 0;
            Vertex m = new Vertex (key.get(),  new Vector <Long>(),centroid, depth);
            for (Vertex v: values){
                if(!v.adjacent.isEmpty()){
                    m.adjacent = v.adjacent;
                }
                if (v.centroid > 0 && v.depth < min_depth){
                    min_depth = v.depth;
                    m.centroid = v.centroid;
                }
            }
            m.depth = min_depth;
            context.write(key,m);
        }
    }
    public static class JobThreeMapperOne extends Mapper<LongWritable, Vertex, LongWritable, LongWritable>{
        @Override
        public void map ( LongWritable key, Vertex value, Context context )
                    throws IOException, InterruptedException{
            
            long temp = 1;
            if (value.centroid >= 0)
                context.write(new LongWritable (value.centroid), new LongWritable(temp));
            
        }
    }

    public static class JobThreeReducerOne extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {
        @Override
        public void reduce ( LongWritable key, Iterable<LongWritable> values, Context context )
                           throws IOException, InterruptedException {
            long m = 0;
            for (LongWritable v: values) {
                m = m + v.get();
            }   
            // System.out.println(m);
            context.write(key,new LongWritable (m));
        }
    }

    public static void main ( String[] args ) throws Exception {
        /* ... First Map-Reduce job to read the graph */
        Job job = Job.getInstance();
        job.setJobName("MyJob");
        job.setJarByClass(GraphPartition.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);
        job.setMapperClass(JobOneMapperOne.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]+"/i0"));
        
        job.waitForCompletion(true);
        
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            job = Job.getInstance();
            /* ... Second Map-Reduce job to do BFS */
            job.setJarByClass(GraphPartition.class);
            job.setMapperClass(JobTwoMapperOne.class);
            job.setReducerClass(JobTwoReducerOne.class);
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(Vertex.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Vertex.class);
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job, new Path(args[1]  +"/i"+i));
            FileOutputFormat.setOutputPath(job,new Path(args[1]  +"/i"+(i+1)));
            job.waitForCompletion(true);
        }
        job = Job.getInstance();
        /* ... Final Map-Reduce job to calculate the cluster sizes */
        job.setJarByClass(GraphPartition.class);
        job.setMapperClass(JobThreeMapperOne.class);
        job.setReducerClass(JobThreeReducerOne.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[1] + "/i8"));
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        job.waitForCompletion(true);
    }
}
