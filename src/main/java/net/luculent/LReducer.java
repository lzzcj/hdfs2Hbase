package net.luculent;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;


import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.acl.AclEntry;

public class LReducer extends TableReducer<LongWritable, Text, NullWritable> {

    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text value : values) {
            String s = value.toString();
            if(!s.contains("num")) {
                continue;
            }

            String[] fields = value.toString().split(",");

            String split = fields[4].replace(".csv","");//测点

            String substring = fields[4].substring(15, 23);

            String rowkey = substring +"_" + fields[1];

            Put put = new Put(Bytes.toBytes(rowkey));//(AIXX_时间)作为rowkey

            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("type"),Bytes.toBytes(fields[0]));//历史类型
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("number"),Bytes.toBytes(fields[2]));//数值
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("status"),Bytes.toBytes(fields[3]));//状态
            put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("filename"),Bytes.toBytes(split));//测点

            context.write(NullWritable.get(),put);
        }
    }
}


