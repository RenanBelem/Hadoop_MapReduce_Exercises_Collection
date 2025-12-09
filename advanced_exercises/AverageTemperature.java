package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        //Registrar as classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);
        j.setCombinerClass(CombineForAverage.class);

        //definir os tipos de saida
        //MAP
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);

        //REDUCE
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        //definir arqs de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        //rodar
        j.waitForCompletion(false);
    }

    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //funçao cahamda autoticamente por linha do arquivo

            //obtendo a linha
            String linha = value.toString();

            //quebranco colunas
            String colunas[] = linha.split(",");


            float temperatura = Float.parseFloat(colunas[8]);
            int qtd = 1;
            String chave = "media";

            //enviando dados no formato (chave, valor) para o reduce
            con.write(new Text(chave),
                    new FireAvgTempWritable(temperatura, qtd));
        }
    }

    public static class CombineForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable>{
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            //somar as teperaturas e as qntd pára cada chave
            float somaTemp = 0.0f;
            int somaQtds = 0;
            for(FireAvgTempWritable o:values){
                somaTemp += o.getSomaTemperaturas();
                somaQtds += o.getQtd();
            }
            //passando para o reduce valores pre somados
            con.write(key, new FireAvgTempWritable(somaTemp, somaQtds));
        }
    }


    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {

            //logica do reduce:
            //recebe diferentes objetos compostos (temperatura, qtd)
            //somar as temperaturas e somas as qntds
            float somaTemps = 0.0f;
            int somaQtds = 0;
            for (FireAvgTempWritable o:values){
                somaTemps += o.getSomaTemperaturas();
                somaQtds += o.getQtd();
            }
            //calcular media
            float media = somaTemps / somaQtds;

            //salvando resultado
            con.write(key, new FloatWritable(media));

        }
    }
}
