package project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.StringWriter;

public class SortDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        // 2 设置jar加载路径
        job.setJarByClass(SortDriver.class);

        // 3 设置map和reduce类
        job.setMapperClass(ReadMapper.class);
        job.setReducerClass(SortReducer.class);
        // 4 设置map输出
        job.setMapOutputKeyClass(SortBeans.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5 设置最终输出kv类型
        job.setOutputKeyClass(StringWriter.class);
        job.setOutputValueClass(NullWritable.class);
        /**
         * 设置最终文件个数
         */
        job.setNumReduceTasks(1);
        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/xurui/lagoutest/Task"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/xurui/lagoutest/Tast_result"));



        // 7 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
