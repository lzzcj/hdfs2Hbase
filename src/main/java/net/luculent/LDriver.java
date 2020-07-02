package net.luculent;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LDriver implements Tool {

    private Configuration configuration = null;

    @Override
    public int run(String[] args) throws Exception {
        //1获取job对象
        Job job = Job.getInstance(configuration);

        //设置驱动类路径
        job.setJarByClass(LDriver.class);

        //设置Mapper和Mapper输出kv类型
        job.setMapperClass(LMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        //设置reducer类
        TableMapReduceUtil.initTableReducerJob("test2",LReducer.class,job);

        //设置输入参数
        FileInputFormat.setInputPaths(job,new Path("hdfs://hacluster/user/edos/*"));

        boolean result = job.waitForCompletion(true);

        return result?0:1;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return configuration;
    }

    public static void main(String[] args) {
        try {

            Configuration configuration = new Configuration();

            int run = ToolRunner.run(configuration, new LDriver(), args);

            System.exit(run);
        }catch (Exception ex){
            ex.printStackTrace();
        }

    }
}
