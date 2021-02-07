import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.Iterator;

import java.io.IOException;

public class Main {
    //Driver Class
    public static void main(String[] args) throws Exception {
        //set up configurations
        Configuration c = new Configuration();
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);

        Job j = new Job(c, "averageCalculator");
        j.setJarByClass(Main.class);
        j.setMapperClass(AverageMap.class);
        j.setReducerClass(Reduce.class);
        j.setCombinerClass(Combiner.class);

        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(Text.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        //get input paths from aruguments
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //start measuring the start time.
        long startTime = System.currentTimeMillis();
        j.waitForCompletion(true);
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("Time Elapsed : " + estimatedTime);
        System.exit(0);
    }


    //Mapper
    public static class AverageMap extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(final LongWritable key, final Text value,
                           final Context context) throws IOException, InterruptedException {
            context.write(new Text("MAPPER"), value);
        }
    }

    //Combiner
    public static class Combiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(final Text key, final Iterable<Text> values,
                              final Context context) throws IOException, InterruptedException {
            Integer count = 0;
            Double sum = 0D;
            final Iterator<Text> itr = values.iterator();
            while (itr.hasNext()) {
                final String text = itr.next().toString();
                final Double value = Double.parseDouble(text);
                count++;
                sum += value;
            }

            final double average = sum / count;

            context.write(new Text("A_C"), new Text(average + "_" + count));
        }
    }

    //Reducer
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(final Text key, final Iterable<Text> values,
                              final Context context) throws IOException, InterruptedException {
            Double sum = 0D;
            Integer totalcount = 0;
            final Iterator<Text> itr = values.iterator();
            while (itr.hasNext()) {
                final String text = itr.next().toString();
                final String[] tokens = text.split("_");
                final Double average = Double.parseDouble(tokens[0]);
                final Integer count = Integer.parseInt(tokens[1]);
                sum += (average * count);
                totalcount += count;
            }

            final double average = sum / totalcount;

            context.write(new Text("AVERAGE"), new Text(Double.toString(average)));
        }
    }
}