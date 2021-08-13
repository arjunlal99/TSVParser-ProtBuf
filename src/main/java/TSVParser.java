import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroup;
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




    public static class TSVParserMapper extends Mapper<LongWritable, Text, Void, Group>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] columns = line.split("\t");
            JSONParser jsonParser = new JSONParser();
            Configuration conf = context.getConfiguration();
            String schema = "message example{\n";
            try{
                JSONArray fields = (JSONArray) jsonParser.parse(conf.get("fields"));
                for(int i=0; i < fields.size(); i++){
                    JSONObject field = (JSONObject) fields.get(i);
                    schema = schema + "required " + (String)field.get("type")+ " " + (String) field.get("name")  +";\n";
                }
                schema = schema + "}";
            }
            catch (ParseException e){
                e.printStackTrace();
            }

            GroupFactory factory = new SimpleGroupFactory(MessageTypeParser.parseMessageType(schema));
            Group group = factory.newGroup();
       //     String fields = conf.get("path");



           // JSONParser jsonParser = new JSONParser();
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

        String schema =  "message example{\n";
        JSONArray fields = (JSONArray) ConfParser.fields;
        for(int i=0; i < fields.size(); i++){
            JSONObject field = (JSONObject) fields.get(i);
       //     schema = schema + "required " + "BINARY " + (String) field.get("name") + " (utf8)" +";\n";
            schema = schema + "required " + (String)field.get("type")+ " " + (String) field.get("name")  +";\n";
        }
        schema = schema + "}";
        ExampleOutputFormat.setSchema(job, MessageTypeParser.parseMessageType(schema));

        ExampleOutputFormat.setCompression(job, CompressionCodecName.UNCOMPRESSED);

        FileInputFormat.addInputPath(job, new Path(ConfParser.inputDir));
        FileOutputFormat.setOutputPath(job, new Path(ConfParser.outputDir));


        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }

}
