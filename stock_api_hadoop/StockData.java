import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StockData {

    public static class StockDataMapper1 extends Mapper < Object, Text, Text, Text > {

        // Input: symbol,name,sector,industry,date,open,high,low,close,adjClose,volume,unadjustedVolume,change,changePercent,vwap,label,changeOverTime

        public void map(Object key, Text value, Context context) throws IOException,
        InterruptedException {
            String[] data = value.toString().split(",");

            if (data[0].equals("symbol")) {
                return;
            }

            double avg = (Double.parseDouble(data[6]) + Double.parseDouble(data[7])) / 2;
            LocalDate localDate = LocalDate.parse(data[4], DateTimeFormatter.ISO_LOCAL_DATE);

            DayOfWeek dayOfWeek = localDate.getDayOfWeek();

            int daysToGoBackward = DayOfWeek.MONDAY.getValue() - dayOfWeek.getValue();

            LocalDate monday = localDate.plusDays(daysToGoBackward);

            String periodStart = monday.format(DateTimeFormatter.ISO_LOCAL_DATE);

            context.write(new Text(data[0] + "," + data[2].replaceAll("\\s", "_") + "," + periodStart), new Text(value + "," + avg + "," + periodStart));
        }
    }


    public static class StockDataReducer1 extends Reducer < Text, Text, Text, Text > {

        public void reduce(Text key, Iterable < Text > values, Context context) throws IOException,
        InterruptedException {

            // Key: symbol,sector,periodStart
            // Value: symbol,name,sector,industry,date,open,high,lo›w,close,adjClose,volume,unadjustedVolume,change,changePercent,vwap,label,changeOverTime,avg,periodStart

            // Output: symbol,sector,periodStart  avgLow,avgHigh,avgClose,avgOpen,avgVolume,avgPrice

            List < List < Double >> weeklyData = new ArrayList < > ();
            for (Text val: values) {
                String[] data = val.toString().split(",");
                Double low = Double.parseDouble(data[7]);
                Double high = Double.parseDouble(data[6]);
                Double close = Double.parseDouble(data[8]);
                Double open = Double.parseDouble(data[5]);
                Double volume = Double.parseDouble(data[10]);
                Double avg = Double.parseDouble(data[17]);

                List < Double > week = new ArrayList < > ();
                week.add(avg);
                week.add(volume);
                week.add(open);
                week.add(close);
                week.add(low);
                week.add(high);

                weeklyData.add(week);
            }

            double avgPrice = weeklyData.stream().mapToDouble(week -> week.get(0)).average().orElse(0);
            double avgVolume = weeklyData.stream().mapToDouble(week -> week.get(1)).average().orElse(0);
            double avgOpen = weeklyData.stream().mapToDouble(week -> week.get(2)).average().orElse(0);
            double avgClose = weeklyData.stream().mapToDouble(week -> week.get(3)).average().orElse(0);
            double avgLow = weeklyData.stream().mapToDouble(week -> week.get(4)).average().orElse(0);
            double avgHigh = weeklyData.stream().mapToDouble(week -> week.get(5)).average().orElse(0);

            context.write(key, new Text(avgLow + "," + avgHigh + "," + avgClose + "," + avgOpen + "," + avgVolume + "," + avgPrice));
        }
    }

    public static class StockDataMapper2 extends Mapper < Object, Text, Text, Text > {

        // Input: symbol,sector,periodStart  avgLow,avgHigh,avgClose,avgOpen,avgVolume,avgPrice

        public void map(Object k, Text value, Context context) throws IllegalArgumentException,
        IOException,
        InterruptedException {
            // ZION,Financial Services,2022-12-26      48.07,49.185,48.7625,48.5475,669875.0,0.004501349999999999

            String[] elements = value.toString().split("\\s");
            String[] key = elements[0].split(",");
            String data = elements[1];

            if (key.length < 3) {
                throw new IllegalArgumentException("cannot parse value: " + value.toString());
            }

            context.write(new Text(key[1] + "," + key[2]), new Text(data));
        }
    }


    public static class StockDataReducer2 extends Reducer < Text, Text, Text, Text > {

        public void reduce(Text key, Iterable < Text > values, Context context) throws IOException,
        InterruptedException {

            // Key: sector,periodStart
            // Value: avgLow,avgHigh,avgClose,avgOpen,avgVolume,avgPrice

            // Output: sector,periodStart  avgLow,avgHigh,avgClose,avgOpen,avgVolume,avgPrice

            List < List < Double >> weeklyData = new ArrayList < > ();
            for (Text val: values) {
                String[] data = val.toString().split(",");
                Double low = Double.parseDouble(data[0]);
                Double high = Double.parseDouble(data[1]);
                Double close = Double.parseDouble(data[2]);
                Double open = Double.parseDouble(data[3]);
                Double volume = Double.parseDouble(data[4]);
                Double price = Double.parseDouble(data[5]);

                List < Double > week = new ArrayList < > ();
                week.add(price);
                week.add(volume);
                week.add(open);
                week.add(close);
                week.add(low);
                week.add(high);

                weeklyData.add(week);
            }

            double avgPrice = weeklyData.stream().mapToDouble(week -> week.get(0)).average().orElse(0);
            double avgVolume = weeklyData.stream().mapToDouble(week -> week.get(1)).average().orElse(0);
            double avgOpen = weeklyData.stream().mapToDouble(week -> week.get(2)).average().orElse(0);
            double avgClose = weeklyData.stream().mapToDouble(week -> week.get(3)).average().orElse(0);
            double avgLow = weeklyData.stream().mapToDouble(week -> week.get(4)).average().orElse(0);
            double avgHigh = weeklyData.stream().mapToDouble(week -> week.get(5)).average().orElse(0);

            context.write(key, new Text(avgLow + "," + avgHigh + "," + avgClose + "," + avgOpen + "," + avgVolume + "," + avgPrice));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String inputPath = args[0];
        String outputPath = args[1];
        String logFileName = args[2];
        if (outputPath.charAt(outputPath.length() - 1) == '/') {
            outputPath = outputPath.substring(0, outputPath.length() - 1);
        }

        Job job1 = Job.getInstance(conf, "Stock Data Job 1");
        job1.setJarByClass(StockData.class);
        job1.setMapperClass(StockDataMapper1.class);
        job1.setReducerClass(StockDataReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(outputPath + "/job1"));

        boolean job1Success = false;

        try {
            long startTime = System.currentTimeMillis();
            job1Success = job1.waitForCompletion(true);
            long endTime = System.currentTimeMillis();
            double elapsedTime = (endTime - startTime) * 1.0 / 1000;
            if (job1Success) {
                log("Job 1 completed successfully in " + elapsedTime + " s", logFileName);
                Job job2 = Job.getInstance(conf, "Stock Data Job 2");
                job2.setJarByClass(StockData.class);
                job2.setMapperClass(StockDataMapper2.class);
                job2.setReducerClass(StockDataReducer2.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job2, new Path(outputPath + "/job1"));
                FileOutputFormat.setOutputPath(job2, new Path(outputPath + "/final"));

                boolean job2Success = false;

                try {
                    long startTime2 = System.currentTimeMillis();
                    job2Success = job2.waitForCompletion(true);
                    long endTime2 = System.currentTimeMillis();
                    double elapsedTime2 = (endTime2 - startTime2) * 1.0 / 1000;
                    if (job2Success) {
                        log("Job 2 completed successfully in " + elapsedTime2 + " s", logFileName);
                    } else {
                        log("Job 2 failed after " + elapsedTime2 + " s", logFileName);
                    }
                } catch (Exception ex) {
                    log("Error while executing job 2. Message: " + ex.getMessage(), logFileName);
                }

                System.exit(job2Success ? 0 : 1);
            } else {
                log("Job 1 failed after " + elapsedTime + " s", logFileName);
            }
        } catch (Exception ex) {
            log("Error while executing job 1. Message: " + ex.getMessage(), logFileName);
        }

        System.exit(1);
    }

    public static void log(String message, String logFileName) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFileName, true))) {
            LocalDateTime now = LocalDateTime.now();
            String timestamp = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            String logMessage = "[" + timestamp + "] " + message;
            System.out.println(logMessage);
            writer.write(logMessage);
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error writing to log file: " + e.getMessage());
        }
    }
}