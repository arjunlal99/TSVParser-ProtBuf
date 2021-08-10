import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.Type;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TSVParser {

    private static final GroupFactory factory = new SimpleGroupFactory(MessageTypeParser.parseMessageType(
            "message example{\n" +
                    "required BINARY x;\n" +
                    "}"
    ));

    public static class TSVParserMapper extends Mapper<LongWritable, Text, Void, Group>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            Group group = factory.newGroup().append("x", value.toString());

            context.write(null, group);
        }

    }

    public static void main (String [] args) throws Exception{

        Job job = Job.getInstance(new Configuration(), "TSVParser");

        job.setJarByClass(TSVParser.class);
        job.setMapperClass(TSVParserMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);
        job.setOutputFormatClass(ExampleOutputFormat.class);

        ExampleOutputFormat.setSchema(job,MessageTypeParser.parseMessageType(
                "message example{\n" +
                        "required BINARY x;\n" +
                        "}"
        ) );

        ExampleOutputFormat.setCompression(job, CompressionCodecName.UNCOMPRESSED);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }

}
