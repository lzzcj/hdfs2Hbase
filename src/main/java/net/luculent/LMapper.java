package net.luculent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;

public class LMapper extends Mapper<LongWritable, Text,LongWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String filename = ((FileSplit) context.getInputSplit()).getPath().getName();


        String s = value.toString() + "," + filename;
        Text text = new Text(s);

        context.write(key,text);



    }


}
