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


import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class TSVParser {

    private static final GroupFactory factory = new SimpleGroupFactory(MessageTypeParser.parseMessageType(
            "message example{\n" +
                    "required BINARY ts;\n" +
                    "required BINARY uid;\n" +
                    "required BINARY id.orig_h;\n" +
                    "required BINARY id.orig_p;\n" +
                   /*
                    "optional BINARY id.resp_h;\n" +
                    "optional BINARY id.resp_p;\n" +
                    "optional BINARY mac;\n" +
                    "optional BINARY assigned_ip;\n" +
                    "optional BINARY lease_time;\n" +
                    "optional BINARY trans_id;\n" +

                    */
                    "}"
    ));

    public static class TSVParserMapper extends Mapper<LongWritable, Text, Void, Group>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] columns = line.split("\t");
            Group group = factory.newGroup();
            Configuration conf = context.getConfiguration();
       //     String fields = conf.get("path");

            JSONParser jsonParser = new JSONParser();
            try{
                JSONArray fields = (JSONArray) jsonParser.parse(conf.get("fields"));
                for(int i=0; i < fields.size(); i++){
                    JSONObject field = (JSONObject) fields.get(i);
                    group.append((String) field.get("name"), columns[Integer.parseInt((String)field.get("index"))]);
                }
            }
            catch (ParseException e){
                e.printStackTrace();
            }

/*
            group.append("ts", columns[0]);
            group.append("uid", columns[1]);
            group.append("id.orig_h", columns[2]);
            group.append("id.orig_p", columns[3]);
*/
/*

            try(FileReader reader = new FileReader(path)){
                Object obj = jsonParser.parse(reader);
                JSONObject file = (JSONObject) obj;
                JSONArray fields = (JSONArray) file.get("fields");

                for(int i=0; i < fields.size(); i++){
                    JSONObject field = (JSONObject) fields.get(i);
                    group.append((String) field.get("name"), columns[(int)field.get("index")]);
                }

            }
            catch (ParseException e){
                e.printStackTrace();
            }
            catch ( IOException e) {
                e.printStackTrace();
            }

*/
            /*
            group.append("ts", fields[0]);
            group.append("uid", fields[1]);
            group.append("id.orig_h", fields[2]);
            group.append("id.orig_p", fields[3]);
            group.append("id.resp_h", fields[4]);
            group.append("id.resp_p", fields[5]);
            group.append("mac", fields[6]);
            group.append("assigned_ip", fields[7]);
            group.append("lease_time", fields[8]);
            group.append("trans_id", fields[9]);
            */
            context.write(null, group);
        }

    }

    public static class ConfParser{

       public static String inputDir, outputDir, outputFilename;
       public static JSONArray fields;
/*
    path -> absolute path

    conf.json
    {
        "inputDir": "/input",
        "outputDir": "/output",
        "outputFilename": "dhcp-log",
        "fields": []
}

 */
       public ConfParser(String path){
            JSONParser jsonParser = new JSONParser();
            try(FileReader reader = new FileReader(path)){
                Object obj = jsonParser.parse(reader);
                JSONObject file = (JSONObject) obj;
                ConfParser.inputDir = file.get("inputDir").toString();
                ConfParser.outputDir = file.get("outputDir").toString();
                ConfParser.outputFilename = file.get("outputFilename").toString();
                ConfParser.fields = (JSONArray) file.get("fields");
             //   System.out.println(ConfParser.fields);
            }
            catch (FileNotFoundException e){
                e.printStackTrace();
            }
            catch (IOException e){
                e.printStackTrace();
            }
            catch (ParseException e){
                e.printStackTrace();
            }
       }
    }

    public static void main (String [] args) throws Exception{
        ConfParser confParser = new ConfParser(args[0]);
        Configuration conf = new Configuration();
        conf.set("fields", String.valueOf(ConfParser.fields));
        Job job = Job.getInstance(conf, "TSVParser");
        job.getConfiguration().set("mapreduce.output.basename", ConfParser.outputFilename);
        job.setJarByClass(TSVParser.class);
        job.setMapperClass(TSVParserMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Void.class);
        job.setOutputValueClass(Group.class);
        job.setOutputFormatClass(ExampleOutputFormat.class);

        ExampleOutputFormat.setSchema(job,MessageTypeParser.parseMessageType(
                "message example{\n" +
                        "required BINARY ts;\n" +
                        "required BINARY uid;\n" +
                        "required BINARY id.orig_h;\n" +
                        "required BINARY id.orig_p;\n" +
                        /*
                        "optional BINARY id.resp_h;\n" +
                        "optional BINARY id.resp_p;\n" +
                        "optional BINARY mac;\n" +
                        "optional BINARY assigned_ip;\n" +
                        "optional BINARY lease_time;\n" +
                        "optional BINARY trans_id;\n" +
                        */
                        "}"
        ) );

        ExampleOutputFormat.setCompression(job, CompressionCodecName.UNCOMPRESSED);

        FileInputFormat.addInputPath(job, new Path(ConfParser.inputDir));
        FileOutputFormat.setOutputPath(job, new Path(ConfParser.outputDir));


        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }

}
